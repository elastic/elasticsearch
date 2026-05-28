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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

class OriginalValuesSemanticFieldValueFetcher extends SemanticFieldValueFetcher {
    private final Set<String> sourcePaths;

    OriginalValuesSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher,
        Set<String> sourcePaths
    ) {
        super(fieldType, bitSetCache, searcher);
        this.sourcePaths = sourcePaths;
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) {
        List<Object> values = new ArrayList<>();
        for (String path : sourcePaths) {
            Object valueObj = source.extractValue(path, null);
            if (valueObj == null) {
                continue;
            }

            values.addAll(SemanticTextUtils.nodeObjectValues(path, valueObj, false));
        }

        return values;
    }
}
