/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * All this code in this package is forked from Lucene and originates from: org.apache.lucene.codecs.lucene90.blocktree package.
 * The reason this is forked is to avoid loading very large {@link org.elasticsearch.index.codec.postings.terms.FieldReader#minTerm} and
 * {@link org.elasticsearch.index.codec.postings.terms.FieldReader#maxTerm} into jvm heap. The size of these terms is unbounded, and
 * at scale this can consume significant jvm heap.
 *
 *
 */
package org.elasticsearch.index.codec.postings.terms;
