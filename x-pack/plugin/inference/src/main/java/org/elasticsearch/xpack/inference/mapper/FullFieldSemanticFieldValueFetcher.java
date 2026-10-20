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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class FullFieldSemanticFieldValueFetcher extends AbstractEmbeddingsLoadingValueFetcher {
    FullFieldSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        super(fieldType, bitSetCache, searcher);
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) throws IOException {
        Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();

        iterateChildDocs(doc, it, offset -> {
            var fullChunks = chunkMap.computeIfAbsent(offset.field(), k -> new ArrayList<>());
            var rawEmbeddings = readRawEmbeddings(embeddingsFieldLoader::write, source.sourceContentType());
            fullChunks.add(
                offset.inputIndex() != null
                    ? new SemanticTextField.Chunk(offset.inputIndex(), rawEmbeddings)
                    : new SemanticTextField.Chunk(offset.start(), offset.end(), rawEmbeddings)
            );
        });

        if (chunkMap.isEmpty()) {
            return List.of();
        }

        return List.of(
            new SemanticTextField(
                false,
                fieldType.name(),
                null,
                new SemanticTextField.InferenceResult(
                    fieldType.getInferenceId(),
                    fieldType.getModelSettings(),
                    fieldType.getChunkingSettings(),
                    chunkMap
                ),
                source.sourceContentType()
            )
        );
    }
}
