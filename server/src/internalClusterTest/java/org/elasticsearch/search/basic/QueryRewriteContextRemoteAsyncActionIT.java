/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryRewriteContextRemoteAsyncActionIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_A = "cluster-a";
    private static final String REMOTE_CLUSTER_B = "cluster-b";

    private static final ConcurrentHashMap<String, AtomicInteger> INSTRUMENTED_ACTION_CALL_MAP = new ConcurrentHashMap<>();

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_A, true, REMOTE_CLUSTER_B, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(TestPlugin.class);
    }

    @Before
    public void clearInstrumentedActionCallMap() {
        INSTRUMENTED_ACTION_CALL_MAP.clear();
    }

    public void testCallRemoteAsyncAction() {
        // TODO: Implement
    }

    public void testInvalidClusterAlias() {
        // TODO: Implement
    }

    public void testRemoteClusterUnavailable() {
        // TODO: Implement
    }

    private static class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> {
        private static final String NAME = "test";

        private final boolean actionAcknowledged;
        private final SetOnce<Boolean> actionAcknowledgedSupplier;

        private static TestQueryBuilder fromXContent(XContentParser parser) {
            return new TestQueryBuilder();
        }

        private TestQueryBuilder() {
            this.actionAcknowledged = false;
            this.actionAcknowledgedSupplier = null;
        }

        private TestQueryBuilder(StreamInput in) throws IOException {
            super(in);
            this.actionAcknowledged = in.readBoolean();
            this.actionAcknowledgedSupplier = null;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            if (actionAcknowledgedSupplier != null) {
                throw new IllegalStateException(
                    "actionAcknowledgedSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?"
                );
            }

            out.writeBoolean(this.actionAcknowledged);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            return new MatchNoDocsQuery();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        protected boolean doEquals(TestQueryBuilder other) {
            return Objects.equals(this.actionAcknowledged, other.actionAcknowledged)
                && Objects.equals(this.actionAcknowledgedSupplier, other.actionAcknowledgedSupplier);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(actionAcknowledged, actionAcknowledgedSupplier);
        }
    }

    private static class InstrumentedAction extends ActionType<InstrumentedAction.Response> {
        private static final InstrumentedAction INSTANCE = new InstrumentedAction();
        private static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(INSTANCE.name(), Response::new);

        private static final String NAME = "cluster:internal/test/instrumented";

        private InstrumentedAction() {
            super(NAME);
        }

        private static class Request extends ActionRequest {
            private Request() {}

            private Request(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                return true;
            }

            @Override
            public int hashCode() {
                return 0;
            }
        }

        private static class Response extends AcknowledgedResponse {
            private Response() {
                super(true);
            }

            private Response(StreamInput in) throws IOException {
                super(in);
            }
        }
    }

    public static class TransportInstrumentedAction extends HandledTransportAction<
        InstrumentedAction.Request,
        InstrumentedAction.Response> {
        private final ClusterService clusterService;

        @Inject
        public TransportInstrumentedAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
            super(
                InstrumentedAction.NAME,
                transportService,
                actionFilters,
                InstrumentedAction.Request::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, InstrumentedAction.Request request, ActionListener<InstrumentedAction.Response> listener) {
            String clusterName = clusterService.getClusterName().value();
            AtomicInteger callCounter = INSTRUMENTED_ACTION_CALL_MAP.computeIfAbsent(clusterName, k -> new AtomicInteger());
            callCounter.incrementAndGet();

            listener.onResponse(new InstrumentedAction.Response());
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin, SearchPlugin {
        public TestPlugin() {}

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(InstrumentedAction.INSTANCE, TransportInstrumentedAction.class));
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<QueryBuilder>(TestQueryBuilder.NAME, TestQueryBuilder::new, TestQueryBuilder::fromXContent));
        }
    }
}
