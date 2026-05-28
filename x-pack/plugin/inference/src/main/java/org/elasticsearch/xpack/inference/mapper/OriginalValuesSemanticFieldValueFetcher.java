/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.search.lookup.Source;

import java.util.List;
import java.util.function.Function;

class OriginalValuesSemanticFieldValueFetcher extends SemanticFieldValueFetcher {
    OriginalValuesSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        super(fieldType, bitSetCache, searcher);
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) {
        Object valueObj = source.extractValue(fieldType.name(), null);
        if (valueObj == null) {
            return List.of();
        }

        return SemanticTextUtils.nodeObjectValues(fieldType.name(), valueObj, false);
    }
}
