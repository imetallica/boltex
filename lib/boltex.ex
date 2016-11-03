defmodule Boltex do
  @moduledoc """
  Elixir library for using the Neo4J Bolt Protocol.

  It supports de- and encoding of Boltex binaries and sending and receiving
  of data using the Bolt protocol.
  """
  @default_opts [
    host: "localhost",
    port: 7687,
  ]

  def start_link(opts) do
    opts = Keyword.merge @default_opts, opts

    DBConnection.start_link Boltex.Connection, opts
  end

  def test(host, port, query, params \\ %{}, auth \\ {}) do
    options = [
      host: host,
      port: port,
      auth: auth
    ]
    query = %Boltex.Query{statement: query}

    {:ok, pid} = DBConnection.start_link Boltex.Connection, options

    IO.inspect DBConnection.execute pid, query, params, [log: &log/1]
  end

  def log(msg) do
    IO.puts "Pool time: #{msg.pool_time / 1_000}"
    IO.puts "Connection time: #{msg.connection_time / 1_000}"
    IO.puts "Decode time: #{(msg.decode_time || 0) / 1_000}"
  end
end
