defmodule Boltex.Connection do
  @moduledoc """
  DBConnection implementation for Boltex.

  Heavily inspired by
  https://github.com/elixir-ecto/db_connection/tree/master/examples/tcp_connection
  """

  use DBConnection

  alias Boltex.{Bolt, Connection, Query, Error}

  @doc "Callback for DBConnection.connect/1"
  def connect(opts) do
    host        = Keyword.fetch!(opts, :host) |> parse_ip_or_hostname
    port        = Keyword.fetch!(opts, :port)
    auth        = Keyword.fetch!(opts, :auth)

    socket_opts = Keyword.get(opts, :socket_options, [])
    timeout     = Keyword.get(opts, :connect_timeout, 5_000)

    enforced_opts = [packet: :raw, mode: :binary, active: false]
    socket_opts   = Enum.reverse socket_opts, enforced_opts

    with {:ok, port} <- :gen_tcp.connect(host, port, socket_opts, timeout),
         :ok         <- Bolt.handshake(:gen_tcp, port),
         :ok         <- Bolt.init(:gen_tcp, port, auth),
         :ok         <- :inet.setopts(port, active: :once)
    do
      {:ok, port}
    else
      error ->
        {:error, Error.exception(error, nil, :connect)}
    end
  end

  @doc "Callback for DBConnection.checkout/1"
  def checkout(port) do
    case :inet.setopts(port, active: false) do
      :ok    -> {:ok, port}
      other  -> other
    end
  end

  @doc "Callback for DBConnection.checkin/1"
  def checkin(port) do
    case :inet.setopts(port, active: :once) do
      :ok    -> {:ok, port}
      other  -> other
    end
  end

  @doc "Callback for DBConnection.handle_execute/1"
  def handle_execute(%Query{statement: statement}, params, _opts, port) do
    case Bolt.run_statement(:gen_tcp, port, statement, params) do
      [{:success, _} | _] = data ->
        {:ok, data, port}

      %Error{type: :cypher_error} = error ->
      # TODO: Ack failure
        {:error, error, port}

      other ->
        {:disconnect, other, port}
    end
  end

  def handle_cast(i) do
    IO.puts "cast #{inspect i}"
  end

  def handle_info({:tcp_closed, sock}, {sock, _} = state) do
    {:disconnect, Error.exception({:recv, :closed}, state, nil), state}
  end
  def handle_info({:tcp_error, sock, reason}, {sock, _} = state) do
    {:disconnect, Error.exception({:recv, reason}, state, nil), state}
  end
  def handle_info(_, state), do: {:ok, state}

  def disconnect(_err, sock) do
    :gen_tcp.close sock

    :ok
  end

  def parse_ip_or_hostname(host) when is_binary(host) do
    host = String.to_charlist host

    case :inet.parse_address(host) do
      {:ok, address}    -> address
      {:error, :einval} -> host
    end
  end
  def parse_ip_or_hostname(host) when is_tuple(host), do: host
end
