/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Distinct;

//
// DISTINCT command
//
// DISTINCT takes no arguments, so we reuse EXPRESSION_MODE which already
// handles PIPE / RP transitions correctly (see METRICS_INFO and TS_INFO
// for the same approach).
//
DEV_DISTINCT : {this.isDevVersion()}? 'distinct' -> pushMode(EXPRESSION_MODE);
