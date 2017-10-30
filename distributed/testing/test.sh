erl -noinput -sname node0@localhost -setcookie election -connect_all true &
erl -noinput -sname node1@localhost -setcookie election -connect_all true &
erl -noinput -sname node2@localhost -setcookie election -connect_all true &
erl -noinput -sname node3@localhost -setcookie election -connect_all true &
erl -noinput -sname node4@localhost -setcookie election -connect_all true &
erl -sname supervisor@localhost -setcookie election -connect_all true
