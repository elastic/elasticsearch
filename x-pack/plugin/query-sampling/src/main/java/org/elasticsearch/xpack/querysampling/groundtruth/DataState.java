/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

/**
 * What the data was like when a ground truth was computed, as far as that ground truth is concerned: the documents that
 * the exact search matched, which are those that have the vector and pass the filters of the query. It is taken by the
 * exact search itself, so that it is of the very data that was scanned.
 * <p>
 * It is a fingerprint and not a copy. Every insert, update and delete changes the sequence number of what is stored, so
 * a ground truth whose data state is not the one of the same search today is out of date. Two numbers are used as
 * neither alone tells all: the number of documents tells the deletes of documents and the sum of their sequence numbers
 * tells the updates.
 *
 * @param documents the number of documents that were matched
 * @param seqNoSum  the sum of the sequence numbers of the documents that were matched
 */
public record DataState(long documents, double seqNoSum) {}
