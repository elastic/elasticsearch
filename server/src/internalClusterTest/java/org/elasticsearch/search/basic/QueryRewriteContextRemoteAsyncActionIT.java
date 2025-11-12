/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class QueryRewriteContextRemoteAsyncActionIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_A = "cluster-a";
    private static final String REMOTE_CLUSTER_B = "cluster-b";

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_A, true, REMOTE_CLUSTER_B, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(TestPlugin.class);
    }

    private static class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> {
        private static final String NAME = "test";

        private static final ParseField FIELD_FIELD = new ParseField("field");
        private static final ParseField QUERY_FIELD = new ParseField("query");

        private static final ConstructingObjectParser<TestQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            false,
            args -> new TestQueryBuilder((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareString(constructorArg(), QUERY_FIELD);
        }

        private static TestQueryBuilder fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        private final String fieldName;
        private final List<String> queries;

        private TestQueryBuilder(String fieldName, String query) {
            this.fieldName = fieldName;
            this.queries = List.of(query);
        }

        private TestQueryBuilder(StreamInput in) throws IOException {
            super(in);
            this.fieldName = in.readString();
            this.queries = in.readCollectionAsImmutableList(StreamInput::readString);
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeStringCollection(queries);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field(FIELD_FIELD.getPreferredName(), fieldName);
            builder.field(QUERY_FIELD.getPreferredName(), queries.getFirst());
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            queries.forEach(q -> boolQuery.should(new MatchQueryBuilder(fieldName, q)));
            return boolQuery.toQuery(context);
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
            return Objects.equals(fieldName, other.fieldName) && Objects.equals(queries, other.queries);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(fieldName, queries);
        }
    }

    private static class EchoRemoteAction extends ActionType<EchoRemoteAction.Response> {
        private static final EchoRemoteAction INSTANCE = new EchoRemoteAction();
        private static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(INSTANCE.name(), Response::new);

        private static final String NAME = "cluster:internal/test/echo";

        private EchoRemoteAction() {
            super(NAME);
        }

        private static class Request extends ActionRequest {
            private final String value;

            private Request(String value) {
                this.value = value;
            }

            private Request(StreamInput in) throws IOException {
                super(in);
                this.value = in.readString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(value);
            }

            @Override
            public ActionRequestValidationException validate() {
                ActionRequestValidationException validationException = null;
                if (value == null) {
                    validationException = addValidationError("value must not be null", validationException);
                }

                return validationException;
            }

            public String getValue() {
                return value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Request request = (Request) o;
                return Objects.equals(value, request.value);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(value);
            }
        }

        private static class Response extends ActionResponse {
            private final String value;

            private Response(String value) {
                this.value = value;
            }

            private Response(StreamInput in) throws IOException {
                this.value = in.readString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(value);
            }

            public String getValue() {
                return value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Response response = (Response) o;
                return Objects.equals(value, response.value);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(value);
            }
        }
    }

    private static class TransportEchoRemoteAction extends HandledTransportAction<EchoRemoteAction.Request, EchoRemoteAction.Response> {
        private final ClusterService clusterService;

        @Inject
        private TransportEchoRemoteAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
            super(
                EchoRemoteAction.NAME,
                transportService,
                actionFilters,
                EchoRemoteAction.Request::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, EchoRemoteAction.Request request, ActionListener<EchoRemoteAction.Response> listener) {
            String clusterName = clusterService.getClusterName().value();
            listener.onResponse(new EchoRemoteAction.Response(request.getValue() + "_" + clusterName));
        }
    }

    private static class TestPlugin extends Plugin implements ActionPlugin, SearchPlugin {
        private TestPlugin() {}

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(EchoRemoteAction.INSTANCE, TransportEchoRemoteAction.class));
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<QueryBuilder>(TestQueryBuilder.NAME, TestQueryBuilder::new, TestQueryBuilder::fromXContent));
        }
    }
}
