defmodule Boltex.Bolt do
  alias Boltex.{Utils, PackStream, Error}
  require Logger

  @recv_timeout    10_000
  @max_chunk_size  65_535

  @user_agent      "Boltex/1.0"
  @hs_magic        << 0x60, 0x60, 0xB0, 0x17 >>
  @hs_version      << 1 :: 32, 0 :: 32, 0 :: 32, 0 :: 32 >>

  @zero_chunk      << 0, 0 >>

  @sig_init        0x01
  @sig_ack_failure 0x0E
  @sig_reset       0x0F
  @sig_run         0x10
  @sig_discard_all 0x2F
  @sig_pull_all    0x3F
  @sig_success     0x70
  @sig_record      0x71
  @sig_ignored     0x7E
  @sig_failure     0x7F

  @summary         ~w(success ignored failure)a

  @moduledoc """
  The Boltex.Bolt module handles the Bolt protocol specific steps (i.e.
  handshake, init) as well as sending and receiving messages and wrapping
  them in chunks.

  It abstracts transportation, expecing the transport layer to define
  send/2 and recv/3 analogous to :gen_tcp.

  ## Shared options

  Functions that allow for options accept these default options:

    * `recv_timeout`: The timeout for receiving a response from the Neo4J s
      server (default: #{@recv_timeout})
  """

  @doc """
  Initiates the handshake between the client and the server.

  ## Options

  See "Shared options" in the documentation of this module.
  """
  def handshake(transport, port, options \\ []) do
    recv_timeout = get_recv_timeout options

    transport.send port, @hs_magic <> @hs_version
    case transport.recv(port, 4, recv_timeout) do
      {:ok, << 1 :: 32 >>} ->
        :ok

      {:ok, other} ->
        {:error, Error.exception(other, port, :handshake)}

      other ->
        {:error, Error.exception(other, port, :handshake)}
    end
  end

  @doc """
  Initialises the connection.

  Expects a transport module (i.e. `gen_tcp`) and a `Port`. Accepts
  authorisation params in the form of {username, password}.

  ## Options

  See "Shared options" in the documentation of this module.

  ## Examples

      iex> Boltex.Bolt.init :gen_tcp, port
      :ok

      iex> Boltex.Bolt.init :gen_tcp, port, {"username", "password"}
      :ok
  """
  def init(transport, port, auth \\ {}, options \\ []) do
    params = auth_params auth
    send_messages transport, port, [{[@user_agent, params], @sig_init}]

    case receive_data(transport, port, options) do
      {:success, %{}} ->
        :ok

      {:failure, response} ->
        {:error, Error.exception(response, port, :init)}

      other ->
        {:error, Error.exception(other, port, :init)}
    end
  end

  defp auth_params({}), do: %{}
  defp auth_params({username, password}) do
    %{
      scheme: "basic",
      principal: username,
      credentials: password
    }
  end

  @doc """
  Sends a list of messages using the Bolt protocol and PackStream encoding.

  Messages have to be in the form of {[messages], signature}.
  """
  def send_messages(transport, port, messages) do
    messages
    |> Enum.map(&generate_binary_message/1)
    |> generate_chunks
    |> Enum.each(&(transport.send(port, &1)))
  end

  defp generate_binary_message({messages, signature}) do
    messages    = List.wrap messages
    struct_size = length messages

    << 0xB :: 4, struct_size :: 4, signature >> <>
    Utils.reduce_to_binary(messages, &PackStream.encode/1)
  end

  defp generate_chunks(messages, chunks \\ [], current_chunk \\ <<>>)
  defp generate_chunks([], chunks, current_chunk) do
    [current_chunk | chunks]
    |> Enum.reverse
  end
  defp generate_chunks([message | messages], chunks, current_chunk)
  when byte_size(current_chunk <> message) <= @max_chunk_size do
    message_size  = byte_size message
    current_chunk =
      current_chunk <>
      << message_size :: 16 >> <>
      message <>
      @zero_chunk

    generate_chunks messages, chunks, current_chunk
  end
  defp generate_chunks([message | messages], chunks, current_chunk) do
    oversized_chunk = current_chunk <> message
    {first, rest}   = binary_part oversized_chunk, 0, @max_chunk_size
    first_size      = byte_size first
    rest_size       = byte_size rest
    current_chunk   = current_chunk <> << first_size :: 16 >> <> first
    new_chunk       = << rest_size :: 16 >> <> rest

    generate_chunks messages, [current_chunk | chunks], new_chunk
  end

  @doc """
  Runs a statement (most likely Cypher statement) and returns a list of the
  records and a summary.

  Records are represented using PackStream's record data type. Their Elixir
  representation is a Keyword with the indexse `:sig` and `:fields`.

  ## Options

  See "Shared options" in the documentation of this module.

  ## Examples

      iex> Boltex.Bolt.run_statement("MATCH (n) RETURN n")
      [
        {:success, %{"fields" => ["n"]}},
        {:record, [sig: 1, fields: [1, "Example", "Labels", %{"some_attribute" => "some_value"}]]},
        {:success, %{"type" => "r"}}
      ]
  """
  def run_statement(transport, port, statement, params \\ %{}, options \\ []) do
    send_messages transport, port, [
      {[statement, params], @sig_run},
      {[nil], @sig_pull_all}
    ]

    case receive_data(transport, port, options) do
      {:success, %{}} = data ->
        [data | (transport |> receive_data(port) |> List.wrap)]

      {:failure, map} ->
        Boltex.Error.exception map, port, :run_statement

      error ->
        error
    end
  end

  @doc """
  Acknowdledge a server error.

  This function is supposed to be called after a failure response has been
  received from the server.

  ## Options

  See "Shared options" in the documentation of this module.
  """
  def ack_failure(transport, port, options) do
    send_messages transport, port, [
      {[nil], @sig_ack_failure}
    ]

    with {:ignored, []}  <- receive_data(transport, port, options),
         {:success, %{}} <- receive_data(transport, port, options),
    do: :ok
  end

  @doc """
  Receives data.

  This function is supposed to be called after a request to the server has been
  made. It receives data chunks, mends them (if they were split between frames)
  and decodes them using PackStream.

  When just a single message is received (i.e. to acknowledge a command), this
  function returns a tuple with two items, the first being the signature and the
  second being the message(s) itself. If a list of messages is received it will
  return a list of the former.

  The same goes for the messages: If there was a single data point in a message
  said data point will be returned by itself. If there were multiple data
  points, the list will be returned.

  The signature is represented as one of the following:

  * `:success`
  * `:record`
  * `:ignored`
  * `:failure`

  ## Options

  See "Shared options" in the documentation of this module.
  """
  def receive_data(transport, port, options \\ [], previous \\ []) do
    with {:ok, data} <- do_receive_data(transport, port, options)
    do
      case unpack(data) do
        {:record, _} = data ->
          receive_data transport, port, options, [data | previous]

        {status, _} = data when status in @summary and previous == [] ->
          data

        {status, _} = data when status in @summary ->
          Enum.reverse [data | previous]

        other ->
          {:error, Error.exception(other, port, :receive_data)}
      end
    else
      other ->
        {:error, "Error receiving data: #{inspect other}"}
    end
  end

  defp do_receive_data(transport, port, options) do
    recv_timeout = get_recv_timeout options

    with {:ok, <<chunk_size :: 16>>} <- transport.recv(port, 2, recv_timeout),
    do:  do_receive_data(transport, port, chunk_size, options, {:ok, <<>>})
  end
  defp do_receive_data(transport, port, chunk_size, options, {:ok, old_data}) do
    recv_timeout = get_recv_timeout options

    with {:ok, data}   <- transport.recv(port, chunk_size, recv_timeout),
         {:ok, marker} <- transport.recv(port, 2, recv_timeout)
    do
      case marker do
        @zero_chunk ->
          {:ok, old_data <> data}

        <<chunk_size :: 16>> ->
          data = old_data <> data
          do_receive_data(transport, port, chunk_size, options, {:ok, data})
      end
    else
      other ->
        Error.exception other, port, :recv
    end
  end
  defp do_receive_data(_, _, _, _, {:error, _} = error), do: error

  @doc """
  Unpacks (or in other words parses) a message.
  """
  def unpack(<< 0x0B :: 4, packages :: 4, status, message :: binary >>) do
    response =
      case PackStream.decode(message) do
        response when packages == 1 ->
          List.first response
        responses ->
          responses
      end

    case status do
      @sig_success -> {:success, response}
      @sig_record  -> {:record,  response}
      @sig_ignored -> {:ignored, response}
      @sig_failure -> {:failure, response}
      other        -> raise "Couldn't decode #{Utils.hex_encode << other >>}"
    end
  end

  defp get_recv_timeout(options) do
    Keyword.get(options, :recv_timeout, @recv_timeout)
  end
end
