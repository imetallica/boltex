defmodule Boltex.Query do
  defstruct statement: ""
end


defimpl DBConnection.Query, for: Boltex.Query do
  def parse(query, _), do: query

  def encode(_query, data, _), do: data

  def decode(_, result, _), do: result
end
