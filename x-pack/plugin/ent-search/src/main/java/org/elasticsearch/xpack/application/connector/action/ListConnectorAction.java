/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorSearchResult;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ListConnectorAction {

    public static final String NAME = "indices:data/read/xpack/connector/list";
    public static final ActionType<ListConnectorAction.Response> INSTANCE = new ActionType<>(NAME);

    private ListConnectorAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final PageParams pageParams;
        private final List<String> indexNames;
        private final List<String> connectorNames;
        private final List<String> connectorServiceTypes;
        private final String connectorSearchQuery;

        private static final ParseField PAGE_PARAMS_FIELD = new ParseField("pageParams");
        private static final ParseField INDEX_NAMES_FIELD = new ParseField("index_names");
        private static final ParseField NAMES_FIELD = new ParseField("names");
        private static final ParseField SEARCH_QUERY_FIELD = new ParseField("query");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.pageParams = new PageParams(in);
            this.indexNames = in.readOptionalStringCollectionAsList();
            this.connectorNames = in.readOptionalStringCollectionAsList();
            this.connectorServiceTypes = in.readOptionalStringCollectionAsList();
            this.connectorSearchQuery = in.readOptionalString();
        }

        public Request(
            PageParams pageParams,
            List<String> indexNames,
            List<String> connectorNames,
            List<String> serviceTypes,
            String connectorSearchQuery
        ) {
            this.pageParams = pageParams;
            this.indexNames = indexNames;
            this.connectorNames = connectorNames;
            this.connectorServiceTypes = serviceTypes;
            this.connectorSearchQuery = connectorSearchQuery;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public List<String> getIndexNames() {
            return indexNames;
        }

        public List<String> getConnectorNames() {
            return connectorNames;
        }

        public List<String> getConnectorServiceTypes() {
            return connectorServiceTypes;
        }

        public String getConnectorSearchQuery() {
            return connectorSearchQuery;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            // Pagination validation is done as part of PageParams constructor

            if (indexNames != null && indexNames.isEmpty() == false) {
                for (String indexName : indexNames) {
                    try {
                        MetadataCreateIndexService.validateIndexOrAliasName(indexName, InvalidIndexNameException::new);
                    } catch (InvalidIndexNameException e) {
                        validationException = addValidationError(e.toString(), validationException);
                    }
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            pageParams.writeTo(out);
            out.writeOptionalStringCollection(indexNames);
            out.writeOptionalStringCollection(connectorNames);
            out.writeOptionalStringCollection(connectorServiceTypes);
            out.writeOptionalString(connectorSearchQuery);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListConnectorAction.Request request = (ListConnectorAction.Request) o;
            return Objects.equals(pageParams, request.pageParams)
                && Objects.equals(indexNames, request.indexNames)
                && Objects.equals(connectorNames, request.connectorNames)
                && Objects.equals(connectorServiceTypes, request.connectorServiceTypes)
                && Objects.equals(connectorSearchQuery, request.connectorSearchQuery);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pageParams, indexNames, connectorNames, connectorServiceTypes, connectorSearchQuery);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ListConnectorAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "list_connector_request",
            p -> new ListConnectorAction.Request(
                (PageParams) p[0],
                (List<String>) p[1],
                (List<String>) p[2],
                (List<String>) p[3],
                (String) p[4]
            )
        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> PageParams.fromXContent(p), PAGE_PARAMS_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), INDEX_NAMES_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), NAMES_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), Connector.SERVICE_TYPE_FIELD);
            PARSER.declareString(optionalConstructorArg(), SEARCH_QUERY_FIELD);
        }

        public static ListConnectorAction.Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(PAGE_PARAMS_FIELD.getPreferredName(), pageParams);
                builder.field(INDEX_NAMES_FIELD.getPreferredName(), indexNames);
                builder.field(NAMES_FIELD.getPreferredName(), connectorNames);
                builder.field(Connector.SERVICE_TYPE_FIELD.getPreferredName(), connectorServiceTypes);
                builder.field(SEARCH_QUERY_FIELD.getPreferredName(), connectorSearchQuery);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField RESULT_FIELD = new ParseField("results");

        final QueryPage<ConnectorSearchResult> queryPage;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.queryPage = new QueryPage<>(in, ConnectorSearchResult::new);
        }

        public Response(List<ConnectorSearchResult> items, Long totalResults) {
            this.queryPage = new QueryPage<>(items, totalResults, RESULT_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            queryPage.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryPage.toXContent(builder, params);
        }

        public QueryPage<ConnectorSearchResult> queryPage() {
            return queryPage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListConnectorAction.Response that = (ListConnectorAction.Response) o;
            return queryPage.equals(that.queryPage);
        }

        @Override
        public int hashCode() {
            return queryPage.hashCode();
        }
    }

}
