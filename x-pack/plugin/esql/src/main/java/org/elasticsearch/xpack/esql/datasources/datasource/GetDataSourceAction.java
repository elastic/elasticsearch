/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Get one or more data sources by name. */
public class GetDataSourceAction extends ActionType<GetDataSourceAction.Response> {

    public static final GetDataSourceAction INSTANCE = new GetDataSourceAction();
    public static final String NAME = EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME;

    private GetDataSourceAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {
        private final String[] names;

        public Request(TimeValue masterNodeTimeout, String[] names) {
            super(masterNodeTimeout);
            this.names = Objects.requireNonNull(names, "names cannot be null");
        }

        public String[] names() {
            return names;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Collection<DataSource> dataSources;

        public Response(Collection<DataSource> dataSources) {
            Objects.requireNonNull(dataSources, "dataSources cannot be null");
            this.dataSources = dataSources;
        }

        public Collection<DataSource> getDataSources() {
            return dataSources;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("data_sources");
            for (DataSource ds : dataSources) {
                builder.startObject();
                builder.field("name", ds.name());
                builder.field("type", ds.type());
                if (ds.description() != null) {
                    builder.field("description", ds.description());
                }
                builder.field("settings", ds.toPresentationMap());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return this.dataSources.equals(response.dataSources);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return dataSources.hashCode();
        }

        @Override
        public String toString() {
            return "GetDataSourceAction.Response" + Arrays.toString(dataSources.stream().map(DataSource::name).toArray(String[]::new));
        }
    }
}
