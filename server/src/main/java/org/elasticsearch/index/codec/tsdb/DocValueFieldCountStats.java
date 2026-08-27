/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Statistics returned after writing one field's doc values.
 *
 * @param numDocsWithField number of documents that have at least one value for this field
 * @param numValues        total number of values across all documents for this field
 */
public record DocValueFieldCountStats(int numDocsWithField, long numValues) {}
