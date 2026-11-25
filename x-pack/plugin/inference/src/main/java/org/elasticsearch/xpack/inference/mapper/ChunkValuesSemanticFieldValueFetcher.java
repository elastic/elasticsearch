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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class ChunkValuesSemanticFieldValueFetcher extends ChildDocIteratingValueFetcher {
    ChunkValuesSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        super(fieldType, bitSetCache, searcher);
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) throws IOException {
        Map<String, SemanticFieldContent> fieldValueMap = new HashMap<>();
        List<Object> chunks = new ArrayList<>();

        iterateChildDocs(doc, it, offset -> {
            SemanticFieldContent semanticFieldContent = fieldValueMap.computeIfAbsent(offset.field(), k -> {
                var valueObj = source.extractValue(offset.field(), null);
                return new SemanticFieldContent(valueObj);
            });

            try {
                chunks.add(semanticFieldContent.resolve(offset));
            } catch (Exception e) {
                throw new IllegalStateException("Invalid content detected for field [" + offset.field() + "]", e);
            }
        });

        return chunks;
    }
}
