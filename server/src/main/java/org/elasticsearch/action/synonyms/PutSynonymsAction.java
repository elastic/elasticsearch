/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class PutSynonymsAction extends ActionType<PutSynonymsAction.Response> {

    public static final PutSynonymsAction INSTANCE = new PutSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/put";

    public PutSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private final String synonymsSetId;
        private final SynonymRule[] synonymRules;

        public static final ParseField SYNONYMS_SET_FIELD = new ParseField(SynonymsManagementAPIService.SYNONYMS_SET_FIELD);
        private static final ConstructingObjectParser<SynonymRule[], Void> PARSER = new ConstructingObjectParser<>("synonyms_set", args -> {
            @SuppressWarnings("unchecked")
            final List<SynonymRule> synonyms = (List<SynonymRule>) args[0];
            return synonyms.toArray(new SynonymRule[synonyms.size()]);
        });

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> SynonymRule.fromXContent(p), SYNONYMS_SET_FIELD);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            this.synonymRules = in.readArray(SynonymRule::new, SynonymRule[]::new);
        }

        public Request(String synonymsSetId, BytesReference content, XContentType contentType) throws IOException {
            this.synonymsSetId = synonymsSetId;
            this.synonymRules = PARSER.apply(XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, contentType), null);
        }

        Request(String synonymsSetId, SynonymRule[] synonymRules) {
            this.synonymsSetId = synonymsSetId;
            this.synonymRules = synonymRules;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isEmpty(synonymsSetId)) {
                validationException = ValidateActions.addValidationError("synonyms set must be specified", validationException);
            }
            for (SynonymRule synonymRule : synonymRules) {
                String error = synonymRule.validate();
                if (error != null) {
                    validationException = ValidateActions.addValidationError(error, validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            out.writeArray(synonymRules);
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public SynonymRule[] synonymRules() {
            return synonymRules;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(synonymsSetId, request.synonymsSetId) && Arrays.equals(synonymRules, request.synonymRules);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId, Arrays.hashCode(synonymRules));
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private final SynonymsManagementAPIService.UpdateSynonymsResult result;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.result = in.readEnum((SynonymsManagementAPIService.UpdateSynonymsResult.class));
        }

        public Response(SynonymsManagementAPIService.UpdateSynonymsResult result) {
            super();
            Objects.requireNonNull(result, "Result must not be null");
            this.result = result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("result", result.name().toLowerCase(Locale.ENGLISH));
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(result);
        }

        @Override
        public RestStatus status() {
            return switch (result) {
                case CREATED -> RestStatus.CREATED;
                default -> RestStatus.OK;
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return result == response.result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }
}
