/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RankVectorsRetrieverBuilderTests extends ESTestCase {

    private final NamedXContentRegistry registry = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of(new RankVectorsPlugin())).getNamedXContents()
    );

    public void testParsingAndExtraction() throws IOException {
        RankVectorsRetrieverBuilder retriever = parse("""
            {
              "rank_vectors": {
                "field": "tokens",
                "query_vector": [[1.0, 0.0], [0.0, 1.0]],
                "k": 2,
                "num_candidates": 10,
                "filter": [
                  { "term": { "language": "en" } }
                ],
                "min_score": 1.5,
                "_name": "late-interaction"
              }
            }
            """);

        RankVectorsQueryBuilder topDocsQuery = (RankVectorsQueryBuilder) retriever.topDocsQuery();
        assertEquals("tokens", topDocsQuery.getFieldName());
        assertEquals(2, topDocsQuery.getK());
        assertEquals(10, topDocsQuery.getNumCandidates());
        assertEquals(List.of(new TermQueryBuilder("language", "en")), topDocsQuery.getFilterQueries());
        assertEquals("late-interaction", topDocsQuery.queryName());

        SearchSourceBuilder source = new SearchSourceBuilder();
        retriever.extractToSearchSourceBuilder(source, true);
        assertEquals(1, source.subSearches().size());
        assertEquals(topDocsQuery, source.subSearches().getFirst().getQueryBuilder());
        assertEquals(1.5f, source.minScore(), 0f);
    }

    public void testXContentRoundTrip() throws IOException {
        RankVectorsRetrieverBuilder retriever = parse("""
            {
              "rank_vectors": {
                "field": "tokens",
                "query_vector": [[1.0, 0.0], [0.0, 1.0]],
                "k": 2,
                "num_candidates": 10
              }
            }
            """);
        assertEquals(retriever, parse(retriever.toString()));
    }

    private RankVectorsRetrieverBuilder parse(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                registry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
            )
        ) {
            return (RankVectorsRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
                parser,
                new RetrieverParserContext(new SearchUsage(), Predicates.never())
            );
        }
    }
}
