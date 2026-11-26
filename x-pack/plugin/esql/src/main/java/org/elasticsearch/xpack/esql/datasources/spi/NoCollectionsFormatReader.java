/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Marker {@link FormatReader} for a format whose rows hold no collections, such as CSV. Separate from
 * {@link NoConfigFormatReader} because having no per-query keys says nothing about the data's shape.
 */
public interface NoCollectionsFormatReader extends FormatReader {
    @Override
    default CollectionReads collectionReads() {
        return CollectionReads.NONE;
    }
}
