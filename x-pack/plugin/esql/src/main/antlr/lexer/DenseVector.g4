/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar DenseVector;

//
// DENSE_VECTOR command
//
DEV_DENSE_VECTOR : {this.isDevVersion()}? 'dense_vector' -> pushMode(EXPRESSION_MODE);
