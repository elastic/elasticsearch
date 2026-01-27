/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

/**
 * Metadata about a posting list for a centroid.
 *
 * @param offset                    The offset of the posting list in the index.
 * @param length                    The length of the posting list in bytes.
 * @param queryCentroidOrdinal      The ordinal of the centroid that relates to the query quantization, may be the same as the centroid
 *                                  used for the document, may be different.
 * @param documentCentroidScore     The score of the document postings centroid and the query vector
 */
public record PostingMetadata(long offset, long length, int queryCentroidOrdinal, float documentCentroidScore) {
    public static final int NO_ORDINAL = -1;
}
