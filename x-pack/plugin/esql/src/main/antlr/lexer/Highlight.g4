/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Highlight;

//
// HIGHLIGHT command
//
// HIGHLIGHT uses EXPRESSION_MODE so the query can be a string literal or a full-text expression.
DEV_HIGHLIGHT : {this.isDevVersion()}? 'highlight' -> pushMode(EXPRESSION_MODE);
