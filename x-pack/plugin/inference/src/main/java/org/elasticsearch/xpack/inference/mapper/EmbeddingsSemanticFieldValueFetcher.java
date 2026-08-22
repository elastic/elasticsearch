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
import java.util.List;
import java.util.function.Function;

/**
 * A {@link org.elasticsearch.index.mapper.ValueFetcher} for the {@code embeddings} fetch format, which returns one parsed embeddings
 * value per chunk. The chunk offsets are not part of the output, so they are not loaded.
 */
class EmbeddingsSemanticFieldValueFetcher extends AbstractEmbeddingsLoadingValueFetcher {
    EmbeddingsSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        super(fieldType, bitSetCache, searcher);
    }

    @Override
    protected boolean loadOffsets() {
        return false;
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) throws IOException {
        List<Object> embeddings = new ArrayList<>();
        iterateChildDocs(
            doc,
            it,
            () -> embeddings.add(readParsedEmbeddings(embeddingsFieldLoader::write, source.sourceContentType(), fieldType))
        );
        return embeddings;
    }
}
