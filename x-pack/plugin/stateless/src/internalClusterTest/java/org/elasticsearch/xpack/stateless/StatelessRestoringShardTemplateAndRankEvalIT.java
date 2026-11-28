/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Search templates and rank evaluation, which run searches, on a stateless cluster while an index is being restored from a snapshot. A
 * restore that asks for its shards to be reported makes their searches fail with a 403; one that does not leaves them failing with a
 * 503.
 */
public class StatelessRestoringShardTemplateAndRankEvalIT extends AbstractStatelessRestoringShardIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MustachePlugin.class);
        plugins.add(RankEvalPlugin.class);
        return plugins;
    }

    private static SearchTemplateRequest searchTemplate(String index) {
        final var request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        request.setScriptType(ScriptType.INLINE);
        request.setScript("{\"query\": {\"match_all\": {}}}");
        request.setScriptParams(Map.of());
        return request;
    }

    // _search/template

    public void testSearchTemplateFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(
                    Exception.class,
                    () -> client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, searchTemplate(RESTORED_INDEX))
                        .actionGet(30, TimeUnit.SECONDS)
                ),
                restoreUuid
            );
        });
    }

    public void testSearchTemplateFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableException(
                expectThrows(
                    Exception.class,
                    () -> client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, searchTemplate(RESTORED_INDEX))
                        .actionGet(30, TimeUnit.SECONDS)
                )
            );
        });
    }

    // _msearch/template

    /**
     * {@code _msearch/template} succeeds, reports the search of the restoring index as a 403 failure, and answers the search of the
     * healthy one.
     */
    public void testMultiSearchTemplateReportsShardRestoringPerSearch() throws Exception {
        whileRestoring(true, true, restoreUuid -> {
            final var request = new MultiSearchTemplateRequest().add(searchTemplate(RESTORED_INDEX)).add(searchTemplate(HEALTHY_INDEX));
            final var response = client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, request).actionGet(30, TimeUnit.SECONDS);
            try {
                assertTrue(response.getResponses()[0].isFailure());
                assertShardRestoringException(response.getResponses()[0].getFailure(), restoreUuid);
                assertFalse(response.getResponses()[1].isFailure());
            } finally {
                response.decRef();
            }
        });
    }

    public void testMultiSearchTemplateReportsServiceUnavailablePerSearchWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(true, false, restoreUuid -> {
            final var request = new MultiSearchTemplateRequest().add(searchTemplate(RESTORED_INDEX)).add(searchTemplate(HEALTHY_INDEX));
            final var response = client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, request).actionGet(30, TimeUnit.SECONDS);
            try {
                assertTrue(response.getResponses()[0].isFailure());
                assertServiceUnavailableException(response.getResponses()[0].getFailure());
                assertFalse(response.getResponses()[1].isFailure());
            } finally {
                response.decRef();
            }
        });
    }

    // _rank_eval

    private static RankEvalRequest rankEval() {
        final var ratedRequest = new RatedRequest(
            "restoring_query",
            List.of(new RatedDocument(RESTORED_INDEX, "id", 1)),
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        );
        return new RankEvalRequest(new RankEvalSpec(List.of(ratedRequest), new PrecisionAtK()), new String[] { RESTORED_INDEX });
    }

    /**
     * {@code _rank_eval} succeeds and reports the rated request whose search hit the restoring shard as a 403 failure.
     */
    public void testRankEvalReportsShardRestoringAsTheFailureOfTheRatedRequest() throws Exception {
        whileRestoring(true, restoreUuid -> {
            try (var response = client().execute(RankEvalPlugin.ACTION, rankEval()).actionGet(30, TimeUnit.SECONDS)) {
                assertThat(response.getFailures().keySet(), equalTo(Set.of("restoring_query")));
                assertShardRestoringException(response.getFailures().get("restoring_query"), restoreUuid);
            }
        });
    }

    public void testRankEvalReportsServiceUnavailableAsTheFailureOfTheRatedRequestWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            try (var response = client().execute(RankEvalPlugin.ACTION, rankEval()).actionGet(30, TimeUnit.SECONDS)) {
                assertThat(response.getFailures().keySet(), equalTo(Set.of("restoring_query")));
                assertServiceUnavailableException(response.getFailures().get("restoring_query"));
            }
        });
    }
}
