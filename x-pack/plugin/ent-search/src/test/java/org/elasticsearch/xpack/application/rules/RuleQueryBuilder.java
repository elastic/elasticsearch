/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that will promote selected documents (identified by ID) above matches produced by an "organic" query. In practice, some upstream
 * system will identify the promotions associated with a user's query string and use this object to ensure these are "pinned" to the top of
 * the other search results.
 */
public class RuleQueryBuilder extends AbstractQueryBuilder<RuleQueryBuilder> {
    public static final String NAME = "rules";

    private static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    private static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    public static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");
    private final List<String> rulesetIds;
    private final Map<String,Object> matchCriteria;
    private QueryBuilder organicQuery;

    public RuleQueryBuilder(QueryBuilder organicQuery, List<String> rulesetIds, Map<String,Object> matchCriteria) {
        if (organicQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] organicQuery cannot be null");
        }
        this.organicQuery = organicQuery;

        if (rulesetIds == null || rulesetIds.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] rulesetIds cannot be null or empty");
        }
        this.rulesetIds = rulesetIds;

        if (matchCriteria == null || matchCriteria.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] matchCriteria cannot be null or empty");
        }
        this.matchCriteria = matchCriteria;
    }

    /**
     * Read from a stream.
     */
    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.organicQuery = in.readNamedWriteable(QueryBuilder.class);
        this.rulesetIds = in.readStringList();
        this.matchCriteria = in.readMap();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(organicQuery);
        out.writeStringCollection(rulesetIds);
        out.writeGenericMap(matchCriteria);
    }

    /**
     * @return the organic query set in the constructor
     */
    public QueryBuilder organicQuery() {
        return this.organicQuery;
    }

    public List<String> rulesetIds() {
        return this.rulesetIds;
    }

    public Map<String,Object> matchCriteria() {
        return this.matchCriteria;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ORGANIC_QUERY_FIELD.getPreferredName());
        organicQuery.toXContent(builder, params);
        builder.array(RULESET_IDS_FIELD.getPreferredName(), rulesetIds.toArray());
        builder.field(MATCH_CRITERIA_FIELD.getPreferredName());
        builder.map(matchCriteria);
        builder.endObject();
    }

    private static final ConstructingObjectParser<RuleQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        QueryBuilder organicQuery = (QueryBuilder) a[0];
        @SuppressWarnings("unchecked")
        List<String> rulesetIds = (List<String>) a[1];
        @SuppressWarnings("unchecked")
        Map<String,Object> matchCriteria = (Map<String,Object>) a[2];
        return new RuleQueryBuilder(organicQuery, rulesetIds, matchCriteria);
    });
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), RULESET_IDS_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
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

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
//        if (curatedIds.isEmpty() == false) {
//            return this;
//        } else if (curatedIdSupplier != null) {
//            List<String> curatedIds = curatedIdSupplier.get();
//            if (curatedIds == null) {
//                return this; // not executed yet
//            } else {
//                return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds, curatedIds);
//            }
//        }
//
//        // This is a POC, when we have a query rules CRUD API we'd be calling that instead of this test search.
//        SearchRequest searchRequest = new SearchRequest("demo-curations");
//        searchRequest.preference("_local");
//        searchRequest.source().query(buildCurationQuery());
//        searchRequest.source(new SearchSourceBuilder().query(buildCurationQuery()));
//
//        SetOnce<List<String>> idSetOnce = new SetOnce<>();
//        queryRewriteContext.registerAsyncAction((client, listener) -> {
//            client.search(searchRequest, ActionListener.wrap(response -> {
//                List<String> ids = new ArrayList<>();
//                for (SearchHit hit : response.getHits().getHits()) {
//                    // No error case handling here for POC
//                    Object actions = Objects.requireNonNull(hit.getSourceAsMap()).get("actions");
//                    List<String> idsArray = ((List<?>) actions).stream()
//                        .map(action -> (Map<?, ?>) action)
//                        .map(actionMap -> actionMap.get("ids"))
//                        .flatMap(_ids -> ((List<?>) _ids).stream())
//                        .map(id -> (String) id)
//                        .toList();
//                    ids.addAll(idsArray);
//                }
//                idSetOnce.set(ids.stream().distinct().toList());
//                listener.onResponse(null);
//            }, listener::onFailure));
//        });
//
//        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
//        RuleQueryBuilder rewritten = new RuleQueryBuilder(newOrganicQuery, matchCriteria, rulesetIds, idSetOnce::get);
//        rewritten.boost(this.boost);
//        return rewritten;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, curatedIds.toArray(new String[0]);
        return pinnedQueryBuilder.toQuery(context);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(organicQuery, rulesetIds, matchCriteria);
    }

    @Override
    protected boolean doEquals(RuleQueryBuilder other) {
        return Objects.equals(rulesetIds, other.rulesetIds)
            && Objects.equals(matchCriteria, other.matchCriteria)
            && Objects.equals(organicQuery, other.organicQuery);
    }

    // TODO update this to 8.10.0
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_9_0;
    }
}
