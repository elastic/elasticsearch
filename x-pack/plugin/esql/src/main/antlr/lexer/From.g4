/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar From;

//
// FROM command
//
FROM : 'from'                 -> pushMode(FROM_MODE);

// TS command
TS : 'ts' -> pushMode(FROM_MODE);

// EXTERNAL command (internal, deprecated, snapshot-only)
DEV_EXTERNAL : 'external' {EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled()}? -> pushMode(FROM_MODE);

mode FROM_MODE;
FROM_PIPE : PIPE -> type(PIPE), popMode;
FROM_COLON : COLON -> type(COLON);
FROM_SELECTOR : CAST_OP -> type(CAST_OP);
FROM_COMMA : COMMA -> type(COMMA);
FROM_ASSIGN : ASSIGN -> type(ASSIGN);
METADATA : 'metadata';

// Support for EXTERNAL command WITH clause - transitions to EXPRESSION_MODE for map parsing
FROM_WITH : WITH -> type(WITH), popMode, pushMode(EXPRESSION_MODE);

// Support for EXTERNAL command parameters
FROM_PARAM : PARAM -> type(PARAM);
FROM_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);

// we need this for EXPLAIN
// change to double popMode to accommodate subqueries in FROM, when see ')' pop out of subquery(default) mode and current command's mode
FROM_RP : RP -> type(RP), popMode, popMode;

// accommodate subQuery inside FROM
FROM_LP : LP -> type(LP), pushMode(DEFAULT_MODE);

// in 8.14 ` were not allowed
// this has been relaxed in 8.15 since " is used for quoting
fragment UNQUOTED_SOURCE_PART
    : ~[:"=|,[\]/() \t\r\n]
    | '/' ~[*/] // allow single / but not followed by another / or * which would start a comment -- used in index pattern date spec
    ;

UNQUOTED_SOURCE
    : UNQUOTED_SOURCE_PART+
    ;

FROM_UNQUOTED_SOURCE : UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
FROM_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);

// A table function in the source list: FROM DATASET(my_source, {"resource": "..."}).
//
// Matches `name(` as lookahead but emits only `name`, rewinding so the '(' is re-lexed in
// TABLE_FUNCTION_MODE. The trailing '(' has to be part of the match because nothing else
// distinguishes a function name from an index pattern -- UNQUOTED_SOURCE matches both -- and a lexer
// rule cannot look at the token it emitted last. Placed after FROM_UNQUOTED_SOURCE for readability
// only: ANTLR picks the longest match, and `name(` is always one character longer than the
// UNQUOTED_SOURCE that `name` alone would produce.
//
// The DEV_ prefix is load-bearing beyond convention: the rule carries no {isDevVersion()}? predicate --
// one on a token starting with LETTER would poison the DFA transition for the first character of
// UNQUOTED_SOURCE, the hottest token in this mode -- so the parser rule does the gating, and
// EsqlParser's error listener strips a DEV_-named token out of a release build's "expecting {...}"
// message by that prefix alone. Without it a GA user would be told the parser expected a token for a
// feature that does not exist on their build.
//
// The name admits fewer characters than UNQUOTED_SOURCE_PART, which also takes index-pattern characters
// such as '*', '-' and '+'. None of those is in UNQUOTED_ID_BODY, so a pattern containing one cannot
// match this rule at all: `foo*(` stays what it was, a source pattern followed by a '(' that opens
// nothing, rather than becoming a call to an unknown function named `foo*`. LETTER also excludes '_',
// so `_a(b)` is likewise not a call.
DEV_TABLE_FUNCTION_NAME
    : LETTER UNQUOTED_ID_BODY* '(' { this.rewindToTokenStart(this.getText().length() - 1); }
      -> pushMode(TABLE_FUNCTION_MODE)
    ;

FROM_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

FROM_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

FROM_WS
    : WS -> channel(HIDDEN)
    ;

// Entered from DEV_TABLE_FUNCTION_NAME, positioned on the '(' that rule peeked at and gave back. The mode
// exists to give that one character a different meaning than it has in FROM_MODE, where FROM_LP opens a
// subquery; it needs no whitespace or comment rules because the rule that pushed it only fires when '('
// is the very next character.
mode TABLE_FUNCTION_MODE;

// Pushes EXPRESSION_MODE ONCE, where EXPRESSION_MODE's own LP pushes it twice. That asymmetry is what
// closes the stack: the matching ')' is lexed in EXPRESSION_MODE, whose RP pops twice, which unwinds
// both EXPRESSION_MODE and this mode and leaves the lexer back in FROM_MODE -- so the rest of the
// source list ( `, bar*`, METADATA, '|' ) goes on lexing as source patterns. Same pairing as
// FROM_LP's single push against FROM_RP's double pop for subqueries.
TABLE_FUNCTION_LP : LP -> type(LP), pushMode(EXPRESSION_MODE);
