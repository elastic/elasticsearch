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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PutSynonymsAction extends ActionType<SynonymUpdateResponse> {

    public static final PutSynonymsAction INSTANCE = new PutSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/put";

    public PutSynonymsAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest {
        private final String synonymsSetId;
        private final SynonymRule[] synonymRules;
        private final boolean refresh;

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
            if (in.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                this.refresh = in.readBoolean();
            } else {
                this.refresh = false;
            }
        }

        public Request(String synonymsSetId, boolean refresh, BytesReference content, XContentType contentType) throws IOException {
            this.synonymsSetId = synonymsSetId;
            this.refresh = refresh;
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, contentType)) {
                this.synonymRules = PARSER.apply(parser, null);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse: " + content.utf8ToString(), e);
            }
        }

        Request(String synonymsSetId, SynonymRule[] synonymRules, boolean refresh) {
            this.synonymsSetId = synonymsSetId;
            this.synonymRules = synonymRules;
            this.refresh = refresh;
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                out.writeBoolean(refresh);
            }
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public boolean refresh() {
            return refresh;
        }

        public SynonymRule[] synonymRules() {
            return synonymRules;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(refresh, request.refresh)
                && Objects.equals(synonymsSetId, request.synonymsSetId)
                && Arrays.equals(synonymRules, request.synonymRules);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId, Arrays.hashCode(synonymRules), refresh);
        }
    }
}
