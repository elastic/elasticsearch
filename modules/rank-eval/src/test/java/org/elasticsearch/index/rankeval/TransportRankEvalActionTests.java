/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;

public final class TransportRankEvalActionTests extends ESTestCase {

    private final Settings settings = Settings.builder()
        .put("path.home", createTempDir().toString())
        .put("node.name", "test-" + getTestName())
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    /**
     * Test that request parameters like indicesOptions or searchType from ranking evaluation request are transfered to msearch request
     */
    public void testTransferRequestParameters() throws Exception {
        String indexName = "test_index";
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(
            new RatedRequest("amsterdam_query", Arrays.asList(new RatedDocument(indexName, "1", 3)), new SearchSourceBuilder())
        );
        RankEvalRequest rankEvalRequest = new RankEvalRequest(
            new RankEvalSpec(specifications, new DiscountedCumulativeGain()),
            new String[] { indexName }
        );
        SearchType expectedSearchType = randomFrom(SearchType.CURRENTLY_SUPPORTED);
        rankEvalRequest.searchType(expectedSearchType);
        IndicesOptions expectedIndicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        rankEvalRequest.indicesOptions(expectedIndicesOptions);

        NodeClient client = new NodeClient(settings, null, TestProjectResolvers.mustExecuteFirst()) {
            @Override
            public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                assertEquals(1, request.requests().size());
                assertEquals(expectedSearchType, request.requests().get(0).searchType());
                assertArrayEquals(new String[] { indexName }, request.requests().get(0).indices());
                assertEquals(expectedIndicesOptions, request.requests().get(0).indicesOptions());
            }
        };

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportRankEvalAction action = new TransportRankEvalAction(
            mock(ActionFilters.class),
            client,
            transportService,
            mock(ScriptService.class),
            NamedXContentRegistry.EMPTY,
            mock(ClusterService.class),
            mock(FeatureService.class)
        );
        action.doExecute(null, rankEvalRequest, null);
    }
}
