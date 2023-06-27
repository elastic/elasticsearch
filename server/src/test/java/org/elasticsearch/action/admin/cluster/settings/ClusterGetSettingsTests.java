/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.DefaultSettingsFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.mockito.Mockito.mock;

public class ClusterGetSettingsTests extends ESTestCase {

    public void testRequestConstruction() {
        final Settings persistentSettings = Settings.builder()
            .put("persistent.foo.filtered", "bar")
            .put("persistent.foo.non_filtered", "baz")
            .build();

        final Settings transientSettings = Settings.builder()
            .put("transient.foo.filtered", "bar")
            .put("transient.foo.non_filtered", "baz")
            .build();

        ClusterGetSettingsAction.Response response = new ClusterGetSettingsAction.Response(persistentSettings, transientSettings, null);

        assertEquals(persistentSettings, response.persistentSettings());
        assertEquals(transientSettings, response.transientSettings());
        assertEquals(Settings.EMPTY, response.settings());
    }

    public void testTransportFilters() throws Exception {
        final SettingsFilter filter = new DefaultSettingsFilter(List.of("persistent.foo.filtered", "transient.foo.filtered"));

        TransportClusterGetSettingsAction action = new TransportClusterGetSettingsAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            filter,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );

        final Settings persistentSettings = Settings.builder()
            .put("persistent.foo.filtered", "bar")
            .put("persistent.foo.non_filtered", "baz")
            .build();

        final Settings transientSettings = Settings.builder()
            .put("transient.foo.filtered", "bar")
            .put("transient.foo.non_filtered", "baz")
            .build();

        final Metadata metadata = Metadata.builder().persistentSettings(persistentSettings).transientSettings(transientSettings).build();
        final ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        final PlainActionFuture<ClusterGetSettingsAction.Response> future = new PlainActionFuture<>();
        action.masterOperation(null, null, clusterState, future);
        assertTrue(future.isDone());

        final ClusterGetSettingsAction.Response response = future.get();

        assertFalse(response.persistentSettings().hasValue("persistent.foo.filtered"));
        assertTrue(response.persistentSettings().hasValue("persistent.foo.non_filtered"));

        assertFalse(response.transientSettings().hasValue("transient.foo.filtered"));
        assertTrue(response.transientSettings().hasValue("transient.foo.non_filtered"));
    }
}
