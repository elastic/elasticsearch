/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that will determine based on query context and configured query rules,
 * whether a query should be modified based on actions specified in matching rules.
 *
 * This iteration will determine if a query should have pinned documents and if so,
 * modify the query accordingly to pin those documents.
 */
public class RuleQueryBuilder extends AbstractQueryBuilder<RuleQueryBuilder> {

    public static final String NAME = "rule_query";

    private static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    private static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic_query");

    private final List<String> rulesetIds;
    private final List<MatchCriteria> matchCriteria;
    private QueryBuilder organicQuery;

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_9_0;
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, List<MatchCriteria> matchCriteria, @Nullable List<String> rulesetIds) {
        // TODO validation
        this.organicQuery = organicQuery;
        this.matchCriteria = (matchCriteria != null ? matchCriteria : Collections.emptyList());
        this.rulesetIds = (rulesetIds != null ? rulesetIds : Collections.emptyList());
    }

    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
        matchCriteria = in.readList(MatchCriteria::new);
        rulesetIds = in.readList(StreamInput::readString);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(organicQuery);
        out.writeList(matchCriteria);
        out.writeStringCollection(rulesetIds);
    }

    public List<String> rulesetIds() { return rulesetIds; }
    public List<MatchCriteria> matchCriteria() { return matchCriteria; }
    public QueryBuilder organicQuery() { return organicQuery; }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ORGANIC_QUERY_FIELD.getPreferredName(), organicQuery);
        builder.startArray(MATCH_CRITERIA_FIELD.getPreferredName());
        for (MatchCriteria criteria : matchCriteria) {
            criteria.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray(RULESET_IDS_FIELD.getPreferredName());
        for (String rulesetId : rulesetIds) {
            builder.value(rulesetId);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, new String[0]);
        return pinnedQueryBuilder.toQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {

        // This is a POC, when we have a query rules CRUD API we'd be calling that instead of this test search.

//        GetQueryRulesRequest request = new GetQueryRulesRequest(organicQuery);
        List<String> ids = new ArrayList<>();
//        queryRewriteContext.registerAsyncAction((client, listener) -> {
//            client.get(request, ActionListener.wrap(response -> {
//                ids.addAll(response.getPinnedIds());
//                listener.onResponse(null);
//            }, listener::onFailure));
//        });

        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        if (ids.isEmpty() == false || newOrganicQuery != organicQuery) {
            PinnedQueryBuilder result = new PinnedQueryBuilder(newOrganicQuery, ids.toArray(new String[0]));
            result.boost(this.boost);
            return result;
        }
        return this;
    }

    @Override
    protected boolean doEquals(RuleQueryBuilder other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (super.equals(other) == false) return false;
        return Objects.equals(rulesetIds, other.rulesetIds)
            && Objects.equals(matchCriteria, other.matchCriteria)
            && Objects.equals(organicQuery, other.organicQuery);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.hashCode(), rulesetIds, matchCriteria, organicQuery);
    }

    private static final ConstructingObjectParser<RuleQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        QueryBuilder organicQuery = (QueryBuilder) a[0];
        @SuppressWarnings("unchecked")
        List<MatchCriteria> matchCriteria = (List<MatchCriteria>) a[1];
        @SuppressWarnings("unchecked")
        List<String> rulesetIds = (List<String>) a[2];
        return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds);
    });
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> MatchCriteria.fromXContent(p), MATCH_CRITERIA_FIELD);
        PARSER.declareStringArray(constructorArg(), RULESET_IDS_FIELD);
        declareStandardFields(PARSER);
    }

    public static RuleQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static final class MatchCriteria implements ToXContentObject, Writeable {

        public static final String NAME = "match_criteria";

        private static final ParseField QUERY_STRING_MATCH_FIELD = new ParseField("query_string_match");

        private final String queryStringMatch;

        MatchCriteria(String queryStringMatch) {
            if (Strings.isNullOrEmpty(queryStringMatch)) {
                throw new IllegalArgumentException("queryStringMatch cannot be null or empty");
            }
            this.queryStringMatch = queryStringMatch;
        }

        MatchCriteria(StreamInput in) throws IOException {
            queryStringMatch = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(queryStringMatch);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(QUERY_STRING_MATCH_FIELD.getPreferredName(), queryStringMatch);
            return builder.endObject();
        }

        public static MatchCriteria fromXContent(XContentParser parser) {
            try {
                return PARSER.apply(parser, null);
            } catch (IllegalArgumentException e) {
                throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
            }
        }

        private static final ConstructingObjectParser<MatchCriteria, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new MatchCriteria((String) a[0])
        );

        static {
            PARSER.declareString(constructorArg(), QUERY_STRING_MATCH_FIELD);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryStringMatch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if ((o instanceof MatchCriteria) == false) {
                return false;
            }
            MatchCriteria other = (MatchCriteria) o;
            return queryStringMatch.equals(other.queryStringMatch);
        }
    }

}
