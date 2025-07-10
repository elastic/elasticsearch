/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraphInfoTransportActionTests extends ESTestCase {

    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        licenseState = mock(MockLicenseState.class);
    }

    public void testAvailable() throws Exception {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        GraphInfoTransportAction featureSet = new GraphInfoTransportAction(
            transportService,
            mock(ActionFilters.class),
            Settings.EMPTY,
            licenseState
        );
        boolean available = randomBoolean();
        when(licenseState.isAllowed(Graph.GRAPH_FEATURE)).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        var usageAction = new GraphUsageTransportAction(
            transportService,
            null,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            Settings.EMPTY,
            licenseState
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(null, null, null, future);
        XPackFeatureUsage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureUsage serializedUsage = new GraphFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.graph.enabled", enabled);
            }
        } else {
            settings.put("xpack.graph.enabled", enabled);
        }

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        GraphInfoTransportAction featureSet = new GraphInfoTransportAction(
            transportService,
            mock(ActionFilters.class),
            settings.build(),
            licenseState
        );
        assertThat(featureSet.enabled(), is(enabled));

        GraphUsageTransportAction usageAction = new GraphUsageTransportAction(
            transportService,
            null,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            settings.build(),
            licenseState
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(null, null, null, future);
        XPackFeatureUsage usage = future.get().getUsage();
        assertThat(usage.enabled(), is(enabled));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureUsage serializedUsage = new GraphFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(enabled));
    }

}
