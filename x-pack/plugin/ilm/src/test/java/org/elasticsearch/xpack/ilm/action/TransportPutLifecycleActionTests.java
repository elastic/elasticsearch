/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils;
import org.elasticsearch.xpack.ilm.PutLifecycleMetadataService;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class TransportPutLifecycleActionTests extends ESTestCase {
    public void testIsNoop() {
        LifecyclePolicy policy1 = LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy");
        LifecyclePolicy policy2 = randomValueOtherThan(policy1, () -> LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy"));

        Map<String, String> headers1 = Map.of("foo", "bar");
        Map<String, String> headers2 = Map.of("foo", "eggplant");

        LifecyclePolicyMetadata existing = new LifecyclePolicyMetadata(policy1, headers1, randomNonNegativeLong(), randomNonNegativeLong());

        assertTrue(PutLifecycleMetadataService.isNoopUpdate(existing, policy1, headers1));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(existing, policy2, headers1));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(existing, policy1, headers2));
        assertFalse(PutLifecycleMetadataService.isNoopUpdate(null, policy1, headers1));
    }

    public void testReservedStateHandler() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        ClusterService mockClusterService = mock(ClusterService.class);
        TransportPutLifecycleAction putAction = new TransportPutLifecycleAction(
            transportService,
            mockClusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(PutLifecycleMetadataService.class)
        );
        assertEquals(ReservedLifecycleAction.NAME, putAction.reservedStateHandlerName().get());

        String json = """
            {
              "policy": {
                "phases": {
                  "warm": {
                    "min_age": "10s",
                    "actions": {
                    }
                  }
                }
              }
            }""";

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            PutLifecycleRequest request = PutLifecycleRequest.parseRequest(new PutLifecycleRequest.Factory() {
                @Override
                public PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy) {
                    return new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
                }

                @Override
                public String getPolicyName() {
                    return "my_timeseries_lifecycle2";
                }
            }, parser);

            assertThat(putAction.modifiedKeys(request), containsInAnyOrder("my_timeseries_lifecycle2"));
        }
    }
}
