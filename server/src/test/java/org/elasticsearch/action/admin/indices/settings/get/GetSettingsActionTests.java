/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

public class GetSettingsActionTests extends ESTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SettingsFilter settingsFilter;
    private final String indexName = "test_index";

    private TestTransportGetSettingsAction getSettingsAction;

    class TestTransportGetSettingsAction extends TransportGetSettingsAction {
        TestTransportGetSettingsAction() {
            super(
                GetSettingsActionTests.this.transportService,
                GetSettingsActionTests.this.clusterService,
                GetSettingsActionTests.this.threadPool,
                settingsFilter,
                new ActionFilters(Collections.emptySet()),
                new Resolver(),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
            );
        }

        @Override
        protected void masterOperation(
            Task task,
            GetSettingsRequest request,
            ClusterState state,
            ActionListener<GetSettingsResponse> listener
        ) {
            ClusterState stateWithIndex = ClusterStateCreationUtils.state(indexName, 1, 1);
            super.masterOperation(task, request, stateWithIndex, listener);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        settingsFilter = new SettingsModule(Settings.EMPTY, emptyList(), emptyList(), emptySet()).getSettingsFilter();
        threadPool = new TestThreadPool("GetSettingsActionTests");
        clusterService = createClusterService(threadPool);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        getSettingsAction = new GetSettingsActionTests.TestTransportGetSettingsAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testIncludeDefaults() {
        GetSettingsRequest noDefaultsRequest = new GetSettingsRequest().indices(indexName);
        ActionTestUtils.execute(getSettingsAction, null, noDefaultsRequest, ActionListener.wrap(noDefaultsResponse -> {
            assertNull(
                "index.refresh_interval should be null as it was never set",
                noDefaultsResponse.getSetting(indexName, "index.refresh_interval")
            );
        }, exception -> { throw new AssertionError(exception); }));

        GetSettingsRequest defaultsRequest = new GetSettingsRequest().indices(indexName).includeDefaults(true);

        ActionTestUtils.execute(getSettingsAction, null, defaultsRequest, ActionListener.wrap(defaultsResponse -> {
            assertNotNull(
                "index.refresh_interval should be set as we are including defaults",
                defaultsResponse.getSetting(indexName, "index.refresh_interval")
            );
        }, exception -> { throw new AssertionError(exception); }));

    }

    public void testIncludeDefaultsWithFiltering() {
        GetSettingsRequest defaultsRequest = new GetSettingsRequest().indices(indexName)
            .includeDefaults(true)
            .names("index.refresh_interval");
        ActionTestUtils.execute(getSettingsAction, null, defaultsRequest, ActionListener.wrap(defaultsResponse -> {
            assertNotNull(
                "index.refresh_interval should be set as we are including defaults",
                defaultsResponse.getSetting(indexName, "index.refresh_interval")
            );
            assertNull(
                "index.number_of_shards should be null as this query is filtered",
                defaultsResponse.getSetting(indexName, "index.number_of_shards")
            );
            assertNull(
                "index.warmer.enabled should be null as this query is filtered",
                defaultsResponse.getSetting(indexName, "index.warmer.enabled")
            );
        }, exception -> { throw new AssertionError(exception); }));
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }

        @Override
        public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
            Index[] out = new Index[request.indices().length];
            for (int x = 0; x < out.length; x++) {
                out[x] = new Index(request.indices()[x], "_na_");
            }
            return out;
        }
    }
}
