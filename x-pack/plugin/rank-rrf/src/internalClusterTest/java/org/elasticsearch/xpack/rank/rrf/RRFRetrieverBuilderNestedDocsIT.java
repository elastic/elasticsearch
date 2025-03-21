/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RRFRetrieverBuilderNestedDocsIT extends RRFRetrieverBuilderIT {

    private static final String LAST_30D_FIELD = "views.last30d";
    private static final String ALL_TIME_FIELD = "views.all";

    @Override
    protected void setupIndex() {
        String mapping = """
            {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "dims": 1,
                  "element_type": "float",
                  "similarity": "l2_norm",
                  "index": true,
                  "index_options": {
                    "type": "hnsw"
                  }
                },
                "text": {
                  "type": "text"
                },
                "doc": {
                  "type": "keyword"
                },
                "topic": {
                  "type": "keyword"
                },
                "views": {
                    "type": "nested",
                    "properties": {
                        "last30d": {
                            "type": "integer"
                        },
                        "all": {
                            "type": "integer"
                        }
                    }
                }
              }
            }
            """;
        createIndex(INDEX, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5)).build());
        admin().indices().preparePutMapping(INDEX).setSource(mapping, XContentType.JSON).get();
        indexDoc(INDEX, "doc_1", DOC_FIELD, "doc_1", TOPIC_FIELD, "technology", TEXT_FIELD, "term", LAST_30D_FIELD, 100);
        indexDoc(
            INDEX,
            "doc_2",
            DOC_FIELD,
            "doc_2",
            TOPIC_FIELD,
            "astronomy",
            TEXT_FIELD,
            "search term term",
            VECTOR_FIELD,
            new float[] { 2.0f },
            LAST_30D_FIELD,
            3
        );
        indexDoc(INDEX, "doc_3", DOC_FIELD, "doc_3", TOPIC_FIELD, "technology", VECTOR_FIELD, new float[] { 3.0f });
        indexDoc(
            INDEX,
            "doc_4",
            DOC_FIELD,
            "doc_4",
            TOPIC_FIELD,
            "technology",
            TEXT_FIELD,
            "term term term term",
            ALL_TIME_FIELD,
            100,
            LAST_30D_FIELD,
            40
        );
        indexDoc(INDEX, "doc_5", DOC_FIELD, "doc_5", TOPIC_FIELD, "science", TEXT_FIELD, "irrelevant stuff");
        indexDoc(
            INDEX,
            "doc_6",
            DOC_FIELD,
            "doc_6",
            TEXT_FIELD,
            "search term term term term term term",
            VECTOR_FIELD,
            new float[] { 6.0f },
            LAST_30D_FIELD,
            15
        );
        indexDoc(
            INDEX,
            "doc_7",
            DOC_FIELD,
            "doc_7",
            TOPIC_FIELD,
            "biology",
            TEXT_FIELD,
            "term term term term term term term",
            VECTOR_FIELD,
            new float[] { 7.0f },
            ALL_TIME_FIELD,
            1000
        );
        refresh(INDEX);
    }

    public void testRRFRetrieverWithNestedQuery() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
        SearchSourceBuilder source = new SearchSourceBuilder();

        // Test standard0 retriever (nested query for views)
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.nestedQuery("views", QueryBuilders.rangeQuery("views.last30d").gte(50), ScoreMode.Max))
                .should(QueryBuilders.nestedQuery("views", QueryBuilders.rangeQuery("views.all").gte(100), ScoreMode.Max))
        );
        SearchRequestBuilder req0 = client().prepareSearch(INDEX).setSource(new SearchSourceBuilder().retriever(standard0));
        SearchResponse resp0 = req0.get();

        // Test standard1 retriever (text + ids)
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.idsQuery().addIds("doc_2", "doc_6"))
                .should(QueryBuilders.matchQuery(TEXT_FIELD, "search"))
        );
        SearchRequestBuilder req1 = client().prepareSearch(INDEX).setSource(new SearchSourceBuilder().retriever(standard1));
        SearchResponse resp1 = req1.get();

        // Test knnRetrieverBuilder
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 6.0f }, null, 10, 100, null, 0.1f);
        SearchRequestBuilder req2 = client().prepareSearch(INDEX).setSource(new SearchSourceBuilder().retriever(knnRetrieverBuilder));
        SearchResponse resp2 = req2.get();

        // Now test the combined RRF retriever
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
            )
        );
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        SearchResponse resp = req.get();

        // Assertions
        assertThat(resp.getHits().getTotalHits().value(), equalTo(2L));
        List<String> returnedDocs = Arrays.stream(resp.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());
        assertThat(returnedDocs, containsInAnyOrder("doc_2", "doc_6"));
    }
}
