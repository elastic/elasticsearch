/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportResolveClusterActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCCSCompatibilityCheck() {
        Settings settings = Settings.builder()
            .put("node.name", TransportResolveClusterActionTests.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportVersion nextTransportVersion = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
        try {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                nextTransportVersion,
                threadPool
            );

            ResolveClusterActionRequest request = new ResolveClusterActionRequest(new String[] { "test" }) {
                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    throw new UnsupportedOperationException(
                        "ResolveClusterAction requires at least version "
                            + TransportVersions.V_8_13_0.toReleaseVersion()
                            + " but was "
                            + out.getTransportVersion().toReleaseVersion()
                    );
                }
            };
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            TransportResolveClusterAction action = new TransportResolveClusterAction(
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                null
            );

            final var ex = asInstanceOf(
                IllegalArgumentException.class,
                safeAwaitFailure(ResolveClusterActionResponse.class, listener -> action.doExecute(null, request, listener))
            );

            assertThat(ex.getMessage(), containsString("not compatible with version"));
            assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
            assertThat(ex.getCause().getMessage(), containsString("ResolveClusterAction requires at least version"));
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testHasNonClosedMatchingIndex() {
        List<ResolveIndexAction.ResolvedIndex> indices = Collections.emptyList();
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));

        // as long as there is one non-closed index it should return true
        indices = new ArrayList<>();
        indices.add(new ResolveIndexAction.ResolvedIndex("foo", null, new String[] { "open" }, ".ds-foo"));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("bar", null, new String[] { "system" }, ".ds-bar"));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("baz", null, new String[] { "system", "open", "hidden" }, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("quux", null, new String[0], null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("wibble", null, new String[] { "system", "closed" }, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        // if only closed indexes are present, should return false
        indices.clear();
        indices.add(new ResolveIndexAction.ResolvedIndex("wibble", null, new String[] { "system", "closed" }, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));
        indices.add(new ResolveIndexAction.ResolvedIndex("wobble", null, new String[] { "closed" }, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));

        // now add a non-closed index and should return true
        indices.add(new ResolveIndexAction.ResolvedIndex("aaa", null, new String[] { "hidden" }, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));
    }
}
