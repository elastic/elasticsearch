/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar EsqlBaseLexer;

@header {
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
}

options {
  superClass=LexerConfig;
  caseInsensitive=true;
}

/*
 * Before modifying this file or the files in the `lexer` subdirectory, please read
 * the section below as changes here  have significant impact in the ANTLR generated
 * code and its consumption in Kibana.
 *
 * A. To add a development command (only available behind in snapshot/dev builds)
 *
 * Since the tokens/modes are in development, add a predicate like this:
 * DEV_MYCOMMAND : 'mycommand' {this.isDevVersion()}? -> ...
 *
 * IMPORTANT: always put the predicate AFTER the literal, never before it. This applies to
 * any predicate, not only isDevVersion(), for example EsqlCapabilities.Cap.X.isEnabled().
 *
 * - A predicate BEFORE the literal (DEV_MYCOMMAND : {this.isDevVersion()}? 'mycommand')
 *   prevents ANTLR from caching the DFA start state of the lexer mode, whatever the first
 *   character is. Every token in that mode is then lexed through ATN simulation, which
 *   slows ALL queries, including those that never use the command.
 * - A predicate AFTER the literal ('mycommand' {this.isDevVersion()}?) only prevents
 *   caching the DFA transition where the predicate is evaluated, i.e. the one that
 *   consumes the last character of the literal. For a keyword, that transition is
 *   reached only by input that spells the keyword, so other queries are not affected.
 *
 * Symbol tokens are different because their characters are common in queries. A trailing
 * predicate is evaluated on the last character of the symbol literal, so it uncaches
 * that transition. For '->' that is the '>' transition, not the initial '-'. Every query
 * containing the symbol then pays for it. A predicate on the ARROW token was reported to
 * cause a severe regression (~13x) for ALL queries.
 * For symbol tokens, keep the lexer rule unconditional and guard the parser rule
 * with {this.isDevVersion()}? instead. See the ARROW token as an example.
 *
 * B. To add a new (production-ready) token
 *
 * Be sure to go through step A (add a development token).
 * Make sure to remove the prefix and conditional before promoting the tokens in
 * production.
 *
 * C. Renaming a token
 *
 * Avoid renaming the token. But if you really have to, please check with the
 * Kibana team as they might be using the generated ANTLR "dictionary".
 *
 * D. To remove a token
 *
 * If the tokens haven't made it to production (and make sure to double check),
 * simply remove them from the grammar.
 * If the tokens get promoted to release, check with the Kibana team the impact
 * they have on the UI (auto-completion, etc...)
 */

/*
 * Import lexer grammars for each command, making sure to import
 * the UNKNOWN_CMD token *last* because it takes over parsing for
 * all other commands.
 */
import ChangePoint,
       Dedup,
       DenseVector,
       Enrich,
       Explain,
       Expression,
       FillNull,
       From,
       Fork,
       Let,
       Fuse,
       Highlight,
       Inline,
       InExpression,
       Into,
       Join,
       Lookup,
       MMR,
       MvExpand,
       Project,
       Promql,
       Rename,
       Set,
       Show,
       UnknownCommand;

LINE_COMMENT
    : '//' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' (MULTILINE_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
