/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Cat;

// CAT command — snapshot / dev builds only.
// After the CAT keyword we reuse FROM_MODE, which already provides UNQUOTED_SOURCE
// for the endpoint name and FROM_PIPE to pop back to default mode on '|'.
DEV_CAT : {this.isDevVersion()}? 'cat' -> pushMode(FROM_MODE);
