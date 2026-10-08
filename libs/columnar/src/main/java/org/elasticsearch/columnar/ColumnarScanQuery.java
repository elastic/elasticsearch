/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

/**
 * Marks a query that answers by looking at the values of a ColumNAR column, rather than by an inverted index.
 *
 * <p>Such a query can reach every document of a segment, and confirming one is paid for in decoded column data, so
 * where it sits in a boolean query matters. {@link ColumnarNegatedQuery} uses the mark to tell an exclusion worth
 * reordering from one Lucene already handles well.
 */
public interface ColumnarScanQuery {}
