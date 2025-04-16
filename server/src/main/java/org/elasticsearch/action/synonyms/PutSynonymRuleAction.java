/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PutSynonymRuleAction extends ActionType<SynonymUpdateResponse> {

    public static final PutSynonymRuleAction INSTANCE = new PutSynonymRuleAction();
    public static final String NAME = "cluster:admin/synonym_rules/put";

    public PutSynonymRuleAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final String synonymsSetId;
        private final SynonymRule synonymRule;
        private final boolean refresh;

        public static final ParseField SYNONYMS_FIELD = new ParseField(SynonymsManagementAPIService.SYNONYMS_FIELD);
        private static final ConstructingObjectParser<SynonymRule, String> PARSER = new ConstructingObjectParser<>(
            "synonyms",
            false,
            (params, synonymRuleId) -> new SynonymRule(synonymRuleId, (String) params[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), SYNONYMS_FIELD);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            this.synonymRule = new SynonymRule(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                this.refresh = in.readBoolean();
            } else {
                this.refresh = true;
            }
        }

        public Request(String synonymsSetId, String synonymRuleId, boolean refresh, BytesReference content, XContentType contentType)
            throws IOException {
            this.synonymsSetId = synonymsSetId;
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, contentType)) {
                this.synonymRule = PARSER.apply(parser, synonymRuleId);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse: " + content.utf8ToString(), e);
            }
            this.refresh = refresh;
        }

        Request(String synonymsSetId, SynonymRule synonymRule, boolean refresh) {
            this.synonymsSetId = synonymsSetId;
            this.synonymRule = synonymRule;
            this.refresh = refresh;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(synonymsSetId)) {
                validationException = ValidateActions.addValidationError("synonyms set must be specified", validationException);
            }
            if (Strings.isNullOrEmpty(synonymRule.id())) {
                validationException = ValidateActions.addValidationError("synonym rule id must be specified", validationException);
            }
            String error = synonymRule.validate();
            if (error != null) {
                validationException = ValidateActions.addValidationError(error, validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            synonymRule.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                out.writeBoolean(refresh);
            }
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public SynonymRule synonymRule() {
            return synonymRule;
        }

        public boolean refresh() {
            return refresh;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(refresh, request.refresh)
                && Objects.equals(synonymsSetId, request.synonymsSetId)
                && Objects.equals(synonymRule, request.synonymRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId, synonymRule, refresh);
        }
    }
}
