defmodule Boltex.ConnectionTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Boltex.Connection

  test "#parse_ip_or_hostname/1" do
    assert Connection.parse_ip_or_hostname("testme.net")  == 'testme.net'
    assert Connection.parse_ip_or_hostname("192.168.0.1") == {192, 168, 0, 1}
    assert Connection.parse_ip_or_hostname({192, 168, 99, 100}) == {192, 168, 99, 100}
  end

  @tag :integration
  test "executes a query successfully" do
    assert {:ok, pid} = DBConnection.start_link Boltex.Connection, [host: "localhost", port: 7687, auth: {"neo4j", "password"}]
    query = %Boltex.Query{statement: "MATCH (n) RETURN n"}
    assert {:ok, [{:success, _} | _]} = DBConnection.execute pid, query, %{}, []
  end

  test "handles failures gracefully" do
    assert {:ok, pid} = DBConnection.start_link Boltex.Connection, [host: "localhost", port: 7687, auth: {"neo4j", "wrong!"}]
    query = %Boltex.Query{statement: "MATCH (n) RETURN n"}

    log = capture_log fn ->
      assert_raise DBConnection.ConnectionError, fn ->
        DBConnection.execute pid, query, %{}, []
      end
    end

    assert log =~ ~r(The client is unauthorized due to authentication failure.)
  end
end
