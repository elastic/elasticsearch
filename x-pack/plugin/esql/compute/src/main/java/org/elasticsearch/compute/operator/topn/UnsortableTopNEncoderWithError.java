/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

/**
 * This version of UnsortableTopNEncoder will throw an IllegalArgumentException
 * with the specified error if the query attempts to use this for sorting.
 */
final class UnsortableTopNEncoderWithError extends UnsortableTopNEncoder {
    private final String error;

    UnsortableTopNEncoderWithError(String error) {
        this.error = error;
    }

    @Override
    public TopNEncoder toSortable() {
        throw new IllegalArgumentException(error);
    }

    @Override
    public String toString() {
        return "UnsortableTopNEncoderWithError";
    }
}
