/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;

import java.util.Map;
import java.util.function.Function;

public abstract class InferenceMetadataFieldsMapper extends MetadataFieldMapper {
    public static final String NAME = "_inference_fields";
    public static final String CONTENT_TYPE = "_inference_fields";

    protected InferenceMetadataFieldsMapper(MappedFieldType inferenceFieldType) {
        super(inferenceFieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public InferenceMetadataFieldType fieldType() {
        return (InferenceMetadataFieldType) super.fieldType();
    }

    public abstract static class InferenceMetadataFieldType extends MappedFieldType {
        public InferenceMetadataFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Map.of());
        }

        public abstract ValueFetcher valueFetcher(
            MappingLookup mappingLookup,
            Function<Query, BitSetProducer> bitSetCache,
            IndexSearcher searcher
        );
    }
}
