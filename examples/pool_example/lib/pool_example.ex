defmodule PoolExample do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      supervisor(DBConnection, [Boltex.Connection, boltex_pool_opts()]),
    ]

    opts = [strategy: :one_for_one, name: PoolExample.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def query(statement, params \\ %{}) do
    name  = boltex_pool_opts[:name]
    pool  = boltex_pool_opts[:pool]
    query = %Boltex.Query{statement: statement}
    opts  = [pool: pool]

    DBConnection.run name, &DBConnection.execute(&1, query, params, []), opts
  end

  defp boltex_pool_opts, do: Application.get_env(:pool_example, __MODULE__)
end
