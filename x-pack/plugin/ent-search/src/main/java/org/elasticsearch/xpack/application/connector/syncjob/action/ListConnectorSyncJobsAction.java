/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobSearchResult;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ListConnectorSyncJobsAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/list";
    public static final ActionType<ListConnectorSyncJobsAction.Response> INSTANCE = new ActionType<>(NAME);

    private ListConnectorSyncJobsAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        public static final ParseField CONNECTOR_ID_FIELD = new ParseField("connector_id");
        private static final ParseField PAGE_PARAMS_FIELD = new ParseField("pageParams");
        private final PageParams pageParams;
        private final String connectorId;
        private final ConnectorSyncStatus connectorSyncStatus;
        private final List<ConnectorSyncJobType> connectorSyncJobTypeList;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.pageParams = new PageParams(in);
            this.connectorId = in.readOptionalString();
            this.connectorSyncStatus = in.readOptionalEnum(ConnectorSyncStatus.class);
            this.connectorSyncJobTypeList = stringToEnumList(in.readOptionalStringCollectionAsList());
        }

        public Request(
            PageParams pageParams,
            String connectorId,
            ConnectorSyncStatus connectorSyncStatus,
            List<ConnectorSyncJobType> connectorSyncJobTypeList
        ) {
            this.pageParams = pageParams;
            this.connectorId = connectorId;
            this.connectorSyncStatus = connectorSyncStatus;
            this.connectorSyncJobTypeList = connectorSyncJobTypeList;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public String getConnectorId() {
            return connectorId;
        }

        public ConnectorSyncStatus getConnectorSyncStatus() {
            return connectorSyncStatus;
        }

        public List<ConnectorSyncJobType> getConnectorSyncJobTypeList() {
            return connectorSyncJobTypeList;
        }

        @Override
        public ActionRequestValidationException validate() {
            // Pagination validation is done as part of PageParams constructor
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            pageParams.writeTo(out);
            out.writeOptionalString(connectorId);
            out.writeOptionalEnum(connectorSyncStatus);
            out.writeOptionalStringCollection(enumToStringList(connectorSyncJobTypeList));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(pageParams, request.pageParams)
                && Objects.equals(connectorId, request.connectorId)
                && connectorSyncStatus == request.connectorSyncStatus
                && connectorSyncJobTypeList == null
                    ? request.connectorSyncJobTypeList == null
                    : connectorSyncJobTypeList.equals(request.connectorSyncJobTypeList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pageParams, connectorId, connectorSyncStatus, connectorSyncJobTypeList);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ListConnectorSyncJobsAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "list_connector_sync_jobs_request",
            p -> new ListConnectorSyncJobsAction.Request(
                (PageParams) p[0],
                (String) p[1],
                p[2] != null ? ConnectorSyncStatus.fromString((String) p[2]) : null,
                p[3] != null ? ((List<String>) p[3]).stream().map(ConnectorSyncJobType::fromString).toList() : null
            )
        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> PageParams.fromXContent(p), PAGE_PARAMS_FIELD);
            PARSER.declareString(optionalConstructorArg(), CONNECTOR_ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), ConnectorSyncJob.STATUS_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), ConnectorSyncJob.JOB_TYPE_FIELD);
        }

        public static ListConnectorSyncJobsAction.Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(PAGE_PARAMS_FIELD.getPreferredName(), pageParams);
                builder.field(CONNECTOR_ID_FIELD.getPreferredName(), connectorId);
                builder.field(ConnectorSyncJob.STATUS_FIELD.getPreferredName(), connectorSyncStatus);
                builder.field(ConnectorSyncJob.JOB_TYPE_FIELD.getPreferredName(), connectorSyncJobTypeList);
            }
            builder.endObject();
            return builder;
        }

        private List<ConnectorSyncJobType> stringToEnumList(List<String> jobTypeList) {
            if (jobTypeList == null) {
                return null;
            }
            return jobTypeList.stream().map(ConnectorSyncJobType::fromString).toList();
        }

        private List<String> enumToStringList(List<ConnectorSyncJobType> jobTypeList) {
            if (jobTypeList == null) {
                return null;
            }
            return jobTypeList.stream().map(ConnectorSyncJobType::name).toList();
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField RESULTS_FIELD = new ParseField("results");

        final QueryPage<ConnectorSyncJobSearchResult> queryPage;

        public Response(StreamInput in) throws IOException {
            this.queryPage = new QueryPage<>(in, ConnectorSyncJobSearchResult::new);
        }

        public Response(List<ConnectorSyncJobSearchResult> items, Long totalResults) {
            this.queryPage = new QueryPage<>(items, totalResults, RESULTS_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            queryPage.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryPage.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(queryPage, response.queryPage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryPage);
        }
    }
}
