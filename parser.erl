-module(parser).
-export([read/1]).

read(Filename) ->
	{ok, Text} = file:read_file(Filename),
 	Data = parse(Text),
	case ( lists:nthtail(length(Data) -1, Data) == [{<<>>}] ) of 
		true -> CleanedData = lists:droplast(Data);
		false -> CleanedData = Data
	end,
 	map(
 		fun({Pid, Address, Name, Priority, Tolerance}) -> 
 		{
 			binary_to_integer(Pid),
			binary_to_list(Address),
			binary_to_list(Name),
			binary_to_integer(Priority),
			binary_to_integer(Tolerance)
 		} end, 
 	CleanedData).

%From 'Learn You Some Erlang'.
map(_, []) -> [];
map(F, [H|T]) -> [F(H)|map(F,T)].

% Code for parsing CSV taken from https://gist.github.com/noss/3979
lines(Text) ->
	lines(<<>>, Text).

lines(Line, <<>>) -> 
	[Line];
lines(Line, <<$\n,Rest/binary>>) -> 
	[Line | lines(<<>>, Rest)];
lines(Line, <<C,Rest/binary>>) -> 
	lines(<<Line/binary, C>>, Rest).

columns(Line) ->
	columns(<<>>, Line).

columns(Col, <<>>) -> 
	[Col];
columns(Col, <<$\t,Rest/binary>>) -> 
	[Col | columns(<<>>, Rest)];
columns(Col, <<C,Rest/binary>>) -> 
	columns(<<Col/binary, C>>, Rest).

parse(Text) ->
	[list_to_tuple(columns(Line)) || Line <- lines(Text)].