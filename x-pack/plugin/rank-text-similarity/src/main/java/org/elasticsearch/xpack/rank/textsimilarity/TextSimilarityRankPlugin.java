/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.textsimilarity;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;

public class TextSimilarityRankPlugin extends Plugin implements SearchPlugin {

    public static final String NAME = "text_similarity_reranker";

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(RankBuilder.class, NAME, TextSimilarityRankBuilder::new)
        );
    }

//    @Override
//    public List<NamedXContentRegistry.Entry> getNamedXContent() {
//        return List.of(new NamedXContentRegistry.Entry(RankBuilder.class, new ParseField(NAME), TextSimilarityRankBuilder::fromXContent));
//    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return List.of(
            new RetrieverSpec<>(new ParseField(NAME), TextSimilarityRankRetrieverBuilder::fromXContent)
        );
    }
}
