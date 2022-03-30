/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
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

    public void testCCSCompatibilityCheck() throws Exception {
        Settings settings = Settings.builder()
            .put("node.name", TransportFieldCapabilitiesActionTests.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        try {
            TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);

            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
            fieldCapsRequest.indexFilter(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    if (out.getVersion().before(Version.CURRENT)) {
                        throw new IllegalArgumentException("This query isn't serializable to nodes before " + Version.CURRENT);
                    }
                }
            });

            IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.getAllMetadataFields()).thenReturn(Collections.singleton("_index"));
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            );
            TransportFieldCapabilitiesAction action = new TransportFieldCapabilitiesAction(
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indicesService,
                null
            );

            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> action.doExecute(null, fieldCapsRequest, ActionListener.noop())
            );

            assertThat(
                ex.getMessage(),
                containsString("[class org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest] is not compatible with version")
            );
            assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
            assertEquals("This query isn't serializable to nodes before " + Version.CURRENT, ex.getCause().getMessage());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }
}
