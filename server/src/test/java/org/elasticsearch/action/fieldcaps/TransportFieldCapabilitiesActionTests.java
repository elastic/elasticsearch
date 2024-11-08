/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportFieldCapabilitiesActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCCSCompatibilityCheck() {
        Settings settings = Settings.builder()
            .put("node.name", TransportFieldCapabilitiesActionTests.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
        try {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                transportVersion,
                threadPool
            );

            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
            fieldCapsRequest.indexFilter(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    if (out.getTransportVersion().before(transportVersion)) {
                        throw new IllegalArgumentException("This query isn't serializable before transport version " + transportVersion);
                    }
                }
            });

            IndicesService indicesService = mock(IndicesService.class);
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            TransportFieldCapabilitiesAction action = new TransportFieldCapabilitiesAction(
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indicesService,
                null
            );

            IllegalArgumentException ex = safeAwaitFailure(
                IllegalArgumentException.class,
                FieldCapabilitiesResponse.class,
                l -> action.doExecute(null, fieldCapsRequest, l)
            );

            assertThat(
                ex.getMessage(),
                containsString("[class org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest] is not compatible with version")
            );
            assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
            assertEquals("This query isn't serializable before transport version " + transportVersion, ex.getCause().getMessage());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }
}
