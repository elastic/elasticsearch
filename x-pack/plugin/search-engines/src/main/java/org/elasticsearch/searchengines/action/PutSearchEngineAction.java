/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PutSearchEngineAction extends ActionType<AcknowledgedResponse> {

    public static final PutSearchEngineAction INSTANCE = new PutSearchEngineAction();
    public static final String NAME = "indices:admin/search_engine/put";

    private PutSearchEngineAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String name;
        private final long startTime;

        private String[] indices;

        private String relevanceSettingsId;

        private static final ObjectParser<Request, RestRequest> PARSER;
        static {
            PARSER = new ObjectParser<>("put_search_engine");
            PARSER.declareField(Request::setIndices, (p) -> {
                List<String> indices = new ArrayList<>();
                while ((p.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                        indices.add(p.text());
                    }
                }
                return indices;
            }, new ParseField("indices"), ObjectParser.ValueType.OBJECT_ARRAY);
            PARSER.declareString(Request::setRelevanceSettingsId, new ParseField("relevance_settings_id"));
        }

        public Request(String name) {
            this(name, System.currentTimeMillis(), new String[0], null);
        }

        public Request(String name, long startTime, String[] indices, String relevanceSettingsId) {
            this.name = name;
            this.startTime = startTime;
            this.indices = indices;
            this.relevanceSettingsId = relevanceSettingsId;
        }

        public String getName() {
            return name;
        }

        public String getRelevanceSettingsId() {
            return relevanceSettingsId;
        }

        public void setRelevanceSettingsId(String relevanceSettingsId) {
            this.relevanceSettingsId = relevanceSettingsId;
        }

        public long getStartTime() {
            return startTime;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.hasText(name) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            // validate engine name using the same rules as index name
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(name, InvalidSearchEngineNameException::new);
            } catch (InvalidSearchEngineNameException x) {
                validationException = ValidateActions.addValidationError(x.getMessage(), validationException);
            }
            if (CollectionUtils.isEmpty(indices)) {
                validationException = ValidateActions.addValidationError("no indices specified", validationException);
            }
            for (String indexName : indices) {
                if (Strings.hasText(indexName) == false) {
                    validationException = ValidateActions.addValidationError("index name can't be empty", validationException);
                }
            }

            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.startTime = in.readVLong();
            this.indices = in.readStringArray();
            this.relevanceSettingsId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeVLong(startTime);
            out.writeStringArray(indices);
            out.writeString(relevanceSettingsId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && startTime == request.startTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, startTime);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        public void setIndices(List<String> indices) {
            String[] arr = new String[indices.size()];
            indices.toArray(arr);
            this.indices = arr;
        }

        public static Request parseRestRequest(RestRequest restRequest) throws IOException {
            final Request request = new Request(restRequest.param("name"));
            XContentParser contentParser = restRequest.contentParser();
            PARSER.parse(contentParser, request, restRequest);
            return request;
        }
    }

    public static class InvalidSearchEngineNameException extends ElasticsearchException {
        public InvalidSearchEngineNameException(String name, String desc) {
            super("Invalid engine name [" + name + "], " + desc);
        }

        public InvalidSearchEngineNameException(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }
    }
}
