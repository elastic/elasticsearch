/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils;

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

        assertTrue(TransportPutLifecycleAction.isNoopUpdate(existing, policy1, headers1));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(existing, policy2, headers1));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(existing, policy1, headers2));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(null, policy1, headers1));
    }

    public void testReservedStateHandler() throws Exception {
        TransportPutLifecycleAction putAction = new TransportPutLifecycleAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            mock(NamedXContentRegistry.class),
            mock(XPackLicenseState.class),
            mock(Client.class)
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
            PutLifecycleAction.Request request = PutLifecycleAction.Request.parseRequest("my_timeseries_lifecycle2", parser);

            assertThat(putAction.modifiedKeys(request), containsInAnyOrder("my_timeseries_lifecycle2"));
        }
    }
}
