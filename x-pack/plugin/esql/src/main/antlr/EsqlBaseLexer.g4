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
 * DEV_MYCOMMAND : {this.isDevVersion()}? 'mycommand' -> ...
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
       Enrich,
       Explain,
       Expression,
       From,
       Fork,
       Join,
       Lookup,
       MvExpand,
       Project,
       Rrf,
       Rename,
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
