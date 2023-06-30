/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.rules.action.GetQueryRulesetAction;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");
    
    static final Set<String> ALLOWED_MATCH_CRITERIA = Set.of("query_string");

    private final List<String> rulesetIds;
    private final Map<String, Object> matchCriteria;
    private final QueryBuilder organicQuery;

    private final List<String> curatedIds;
    private final Supplier<List<String>> curatedIdSupplier;
    private final List<Item> curatedDocs;
    private final Supplier<List<Item>> curatedDocsSupplier;

    private final Logger logger = LogManager.getLogger(RuleQueryBuilder.class);

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_500_024;
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, Map<String, Object> matchCriteria, List<String> rulesetIds) {
        this(organicQuery, matchCriteria, rulesetIds, null, null, null, null);
    }

    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
        matchCriteria = in.readMap();
        rulesetIds = in.readList(StreamInput::readString);
        curatedIds = in.readBoolean() ? in.readImmutableList(StreamInput::readString) : null;
        curatedIdSupplier = null;
        curatedDocs = in.readBoolean() ? in.readList(Item::new) : null;
        curatedDocsSupplier = null;
    }

    private RuleQueryBuilder(
        QueryBuilder organicQuery,
        Map<String, Object> matchCriteria,
        @Nullable List<String> rulesetIds,
        List<String> curatedIds,
        List<Item> curatedDocs,
        Supplier<List<String>> curatedIdSupplier,
        Supplier<List<Item>> curatedDocsSupplier

    ) {
        if (organicQuery == null) {
            throw new IllegalArgumentException("organicQuery must not be null");
        }
        if (matchCriteria == null || matchCriteria.isEmpty()) {
            throw new IllegalArgumentException("matchCriteria must not be null or empty");
        }
        for (String matchCriteriaKey : matchCriteria.keySet()) {
            if (ALLOWED_MATCH_CRITERIA.contains(matchCriteriaKey) == false) {
                throw new IllegalArgumentException("matchCriteria key [" + matchCriteriaKey + "] is not allowed");
            }
        }
        if (rulesetIds == null || rulesetIds.isEmpty()) {
            throw new IllegalArgumentException("rulesetIds must not be null or empty");
        }

        this.organicQuery = organicQuery;
        this.matchCriteria = matchCriteria;
        this.rulesetIds = rulesetIds;
        this.curatedIds = curatedIds;
        this.curatedIdSupplier = curatedIdSupplier;
        this.curatedDocs = curatedDocs;
        this.curatedDocsSupplier = curatedDocsSupplier;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (curatedIdSupplier != null) {
            throw new IllegalStateException("curatedIdSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        if (curatedDocsSupplier != null) {
            throw new IllegalStateException("curatedDocsSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }

        out.writeNamedWriteable(organicQuery);
        out.writeGenericMap(matchCriteria);
        out.writeStringCollection(rulesetIds);
        if (curatedIds != null) {
            out.writeBoolean(true);
            out.writeStringCollection(curatedIds);
        } else {
            out.writeBoolean(false);
        }
        if (curatedDocs != null) {
            out.writeBoolean(true);
            out.writeList(curatedDocs);
        } else {
            out.writeBoolean(false);
        }
    }

    public List<String> rulesetIds() {
        return rulesetIds;
    }

    public Map<String, Object> matchCriteria() {
        return matchCriteria;
    }

    public QueryBuilder organicQuery() {
        return organicQuery;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ORGANIC_QUERY_FIELD.getPreferredName(), organicQuery);
        builder.startObject(MATCH_CRITERIA_FIELD.getPreferredName());
        builder.mapContents(matchCriteria);
        builder.endObject();
        builder.startArray(RULESET_IDS_FIELD.getPreferredName());
        for (String rulesetId : rulesetIds) {
            builder.value(rulesetId);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if ((curatedIds != null && curatedIds.isEmpty() == false) && (curatedDocs != null && curatedDocs.isEmpty() == false)) {
            throw new IllegalArgumentException("Can't have both curatedIds and curatedDocs");
        }

        if (curatedIds != null && curatedIds.isEmpty() == false) {
            PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, curatedIds.toArray(new String[0]));
            return pinnedQueryBuilder.toQuery(context);
        } else if (curatedDocs != null && curatedDocs.isEmpty() == false) {
            PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, curatedDocs.toArray(new Item[0]));
            return pinnedQueryBuilder.toQuery(context);
        } else {
            return organicQuery.toQuery(context);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (curatedIds != null || curatedDocs != null) {
            return this;
        } else if (curatedIdSupplier != null || curatedDocsSupplier != null) {
            List<String> curatedIds = curatedIdSupplier != null ? curatedIdSupplier.get() : null;
            List<Item> curatedDocs = curatedDocsSupplier != null ? curatedDocsSupplier.get() : null;
            if (curatedIds == null && curatedDocs == null) {
                return this; // not executed yet
            } else {
                return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds, curatedIds, curatedDocs, null, null);
            }
        }

        // Identify matching rules and apply them if applicable
        SetOnce<List<String>> idSetOnce = new SetOnce<>();
        SetOnce<List<Item>> docsSetOnce = new SetOnce<>();
        List<String> pinnedIds = new ArrayList<>();
        List<Item> pinnedDocs = new ArrayList<>();
        // TODO - Support more than one ruleset ID or take out of scope for MVP (Maybe it's not needed?)
        // We probably should refactor this to a List call and filter by ruleset name,
        // but in that case we may need to return more details for the listed rulesets to avoid a second call
        String rulesetId = rulesetIds.get(0);
        GetQueryRulesetAction.Request getQueryRulesetRequest = new GetQueryRulesetAction.Request(rulesetId);
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            client.execute(GetQueryRulesetAction.INSTANCE, getQueryRulesetRequest, new ActionListener<>() {
                // TODO - Make this less ugly, don't hard code field names, etc.
                @Override
                public void onResponse(GetQueryRulesetAction.Response response) {
                    QueryRuleset queryRuleset = response.queryRuleset();
                    for (QueryRule rule : queryRuleset.rules()) {
                        if (rule.type() == QueryRule.QueryRuleType.PINNED) {
                            for (QueryRuleCriteria criterion : rule.criteria()) {
                                for (String match : matchCriteria.keySet()) {
                                    if (criterion.criteriaMetadata().equals(match) // TODO need to verify "exact" here too
                                        && criterion.criteriaValue().equals(matchCriteria.get(match))) {
                                        if (rule.actions().containsKey("ids")) {
                                            pinnedIds.addAll((List<String>) rule.actions().get("ids"));
                                        } else if (rule.actions().containsKey("docs")) {
                                            Object docsConfiguredInRule = rule.actions().get("docs");
                                            if ((docsConfiguredInRule instanceof List) == false) {
                                                throw new IllegalArgumentException("docs must be a list");
                                            }

                                            List<LinkedHashMap<String, String>> maps = (ArrayList<
                                                LinkedHashMap<String, String>>) docsConfiguredInRule;
                                            List<Item> items = maps.stream()
                                                .map(map -> new Item(map.get("_index"), map.get("_id")))
                                                .toList();
                                            pinnedDocs.addAll(items);

                                        } else {
                                            throw new UnsupportedOperationException("Pinned rules must have id or docs");
                                        }
                                    }
                                }
                            }
                        } else {
                            logger.warn("Skipping unsupported query rule type [" + rule.type() + "]");
                        }
                    }
                    idSetOnce.set(pinnedIds.stream().distinct().toList());
                    docsSetOnce.set(pinnedDocs.stream().distinct().toList());
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    // Ruleset not found, no rules to apply
                    idSetOnce.set(Collections.emptyList());
                    docsSetOnce.set(Collections.emptyList());
                    listener.onResponse(null);
                }
            });
        });

        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        RuleQueryBuilder rewritten = new RuleQueryBuilder(
            newOrganicQuery,
            matchCriteria,
            rulesetIds,
            null,
            null,
            idSetOnce::get,
            docsSetOnce::get
        );
        rewritten.boost(this.boost);
        return rewritten;
    }

    @Override
    protected boolean doEquals(RuleQueryBuilder other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        return Objects.equals(rulesetIds, other.rulesetIds)
            && Objects.equals(matchCriteria, other.matchCriteria)
            && Objects.equals(organicQuery, other.organicQuery)
            && Objects.equals(curatedIds, other.curatedIds)
            && Objects.equals(curatedDocs, other.curatedDocs)
            && Objects.equals(curatedIdSupplier, other.curatedIdSupplier)
            && Objects.equals(curatedDocsSupplier, other.curatedDocsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rulesetIds, matchCriteria, organicQuery, curatedIds, curatedDocs, curatedIdSupplier, curatedDocsSupplier);
    }

    private static final ConstructingObjectParser<RuleQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        QueryBuilder organicQuery = (QueryBuilder) a[0];
        @SuppressWarnings("unchecked")
        Map<String, Object> matchCriteria = (Map<String, Object>) a[1];
        @SuppressWarnings("unchecked")
        List<String> rulesetIds = (List<String>) a[2];
        return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds);
    });
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
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

}
