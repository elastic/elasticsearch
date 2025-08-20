/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TestQueryRulesetAction {

    // TODO - We'd like to transition this to require less stringent permissions
    public static final ActionType<Response> TYPE = new ActionType<>("cluster:admin/xpack/query_rules/test");

    public static final String NAME = TYPE.name();
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private TestQueryRulesetAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements ToXContentObject, IndicesRequest {
        private final String rulesetId;
        private final Map<String, Object> matchCriteria;

        private static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");
        private static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
            this.matchCriteria = in.readGenericMap();
        }

        public Request(String rulesetId, Map<String, Object> matchCriteria) {
            this.rulesetId = rulesetId;
            this.matchCriteria = matchCriteria;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(rulesetId)) {
                validationException = addValidationError("ruleset_id missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
            out.writeGenericMap(matchCriteria);
        }

        public String rulesetId() {
            return rulesetId;
        }

        public Map<String, Object> matchCriteria() {
            return matchCriteria;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(rulesetId, request.rulesetId) && Objects.equals(matchCriteria, request.matchCriteria);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rulesetId, matchCriteria);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
            builder.startObject(MATCH_CRITERIA_FIELD.getPreferredName());
            builder.mapContents(matchCriteria);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "test_query_ruleset_request",
            false,
            (p, name) -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> matchCriteria = (Map<String, Object>) p[0];
                return new Request(name, matchCriteria);
            }

        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
            PARSER.declareString(optionalConstructorArg(), RULESET_ID_FIELD); // Required for parsing
        }

        public static Request parse(XContentParser parser, String name) {
            return PARSER.apply(parser, name);
        }

        @Override
        public String[] indices() {
            return new String[] { QueryRulesIndexService.QUERY_RULES_ALIAS_NAME };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.lenientExpandHidden();
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final int totalMatchedRules;
        private final List<MatchedRule> matchedRules;

        private static final ParseField TOTAL_MATCHED_RULES_FIELD = new ParseField("total_matched_rules");
        private static final ParseField MATCHED_RULES_FIELD = new ParseField("matched_rules");

        public Response(StreamInput in) throws IOException {
            this.totalMatchedRules = in.readVInt();
            this.matchedRules = in.readCollectionAsList(MatchedRule::new);
        }

        public Response(int totalMatchedRules, List<MatchedRule> matchedRules) {
            this.totalMatchedRules = totalMatchedRules;
            this.matchedRules = matchedRules;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(totalMatchedRules);
            out.writeCollection(matchedRules, (stream, matchedRule) -> matchedRule.writeTo(stream));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOTAL_MATCHED_RULES_FIELD.getPreferredName(), totalMatchedRules);
            builder.startArray(MATCHED_RULES_FIELD.getPreferredName());
            for (MatchedRule matchedRule : matchedRules) {
                builder.startObject();
                builder.field("ruleset_id", matchedRule.rulesetId());
                builder.field("rule_id", matchedRule.ruleId());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(totalMatchedRules, response.totalMatchedRules) && Objects.equals(matchedRules, response.matchedRules);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalMatchedRules, matchedRules);
        }
    }

    public record MatchedRule(String rulesetId, String ruleId) {
        public MatchedRule(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(rulesetId);
            out.writeString(ruleId);
        }
    }
}
