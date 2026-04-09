/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class RankFeaturesMapperIntegrationIT extends ESIntegTestCase {

    private static final String LOWER_RANKED_FEATURE = "ten";
    private static final String HIGHER_RANKED_FEATURE = "twenty";
    private static final String INDEX_NAME = "rank_feature_test";
    private static final String FIELD_NAME = "all_rank_features";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperExtrasPlugin.class);
    }

    public void testRankFeaturesTermQuery() throws IOException {
        init();
        assertNoFailuresAndResponse(
            prepareSearch(INDEX_NAME).setQuery(QueryBuilders.termQuery(FIELD_NAME, HIGHER_RANKED_FEATURE)),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(20f));
                }
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch(INDEX_NAME).setQuery(QueryBuilders.termQuery(FIELD_NAME, HIGHER_RANKED_FEATURE).boost(100f)),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(2000f));
                }
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch(INDEX_NAME).setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery(FIELD_NAME, HIGHER_RANKED_FEATURE))
                    .should(QueryBuilders.termQuery(FIELD_NAME, LOWER_RANKED_FEATURE).boost(3f))
                    .minimumShouldMatch(1)
            ),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(3L));
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    if (hit.getId().equals("all")) {
                        assertThat(hit.getScore(), equalTo(50f));
                    }
                    if (hit.getId().equals("lower")) {
                        assertThat(hit.getScore(), equalTo(30f));
                    }
                    if (hit.getId().equals("higher")) {
                        assertThat(hit.getScore(), equalTo(20f));
                    }
                }
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch(INDEX_NAME).setQuery(QueryBuilders.termQuery(FIELD_NAME, "missing_feature")),
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(0L))
        );
    }

    private void init() throws IOException {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        prepareCreate(INDEX_NAME).setSettings(settings)
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("all_rank_features")
                    .field("type", "rank_features")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        BulkResponse bulk = client().prepareBulk()
            .add(
                prepareIndex(INDEX_NAME).setId("all")
                    .setSource(Map.of("all_rank_features", Map.of(LOWER_RANKED_FEATURE, 10, HIGHER_RANKED_FEATURE, 20)))
            )
            .add(prepareIndex(INDEX_NAME).setId("lower").setSource(Map.of("all_rank_features", Map.of(LOWER_RANKED_FEATURE, 10))))
            .add(prepareIndex(INDEX_NAME).setId("higher").setSource(Map.of("all_rank_features", Map.of(HIGHER_RANKED_FEATURE, 20))))
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));
    }

}
