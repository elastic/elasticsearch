/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar UnknownCommand;

//
// Catch-all for unrecognized commands
//
UNKNOWN_CMD : ~[ \r\n\t[\]/]+ -> pushMode(EXPRESSION_MODE) ;
