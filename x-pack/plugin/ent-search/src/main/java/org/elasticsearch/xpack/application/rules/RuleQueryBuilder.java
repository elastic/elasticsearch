/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

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
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    /**
     * Defines the set of allowed match criteria, so that we can validate that rule query requests are sending in allowed/supported data.
     */
    static final Set<String> ALLOWED_MATCH_CRITERIA = Set.of("query_string");

    private final List<String> rulesetIds;
    private final Map<String, Object> matchCriteria;
    private final QueryBuilder organicQuery;

    private final List<String> pinnedIds;
    private final Supplier<List<String>> pinnedIdsSupplier;
    private final List<Item> pinnedDocs;
    private final Supplier<List<Item>> pinnedDocsSupplier;

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
        pinnedIds = in.readBoolean() ? in.readImmutableList(StreamInput::readString) : null;
        pinnedIdsSupplier = null;
        pinnedDocs = in.readBoolean() ? in.readList(Item::new) : null;
        pinnedDocsSupplier = null;
    }

    private RuleQueryBuilder(
        QueryBuilder organicQuery,
        Map<String, Object> matchCriteria,
        List<String> rulesetIds,
        List<String> pinnedIds,
        List<Item> pinnedDocs,
        Supplier<List<String>> pinnedIdsSupplier,
        Supplier<List<Item>> pinnedDocsSupplier

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
        for (String rulesetId : rulesetIds) {
            if (Strings.isNullOrEmpty(rulesetId)) {
                throw new IllegalArgumentException("rulesetId must not be null or empty");
            }
        }

        this.organicQuery = organicQuery;
        this.matchCriteria = matchCriteria;
        this.rulesetIds = rulesetIds;
        this.pinnedIds = pinnedIds;
        this.pinnedIdsSupplier = pinnedIdsSupplier;
        this.pinnedDocs = pinnedDocs;
        this.pinnedDocsSupplier = pinnedDocsSupplier;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (pinnedIdsSupplier != null) {
            throw new IllegalStateException("pinnedIdsSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        if (pinnedDocsSupplier != null) {
            throw new IllegalStateException("pinnedDocsSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }

        out.writeNamedWriteable(organicQuery);
        out.writeGenericMap(matchCriteria);
        out.writeStringCollection(rulesetIds);
        if (pinnedIds != null) {
            out.writeBoolean(true);
            out.writeStringCollection(pinnedIds);
        } else {
            out.writeBoolean(false);
        }
        if (pinnedDocs != null) {
            out.writeBoolean(true);
            out.writeList(pinnedDocs);
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
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if ((pinnedIds != null && pinnedIds.isEmpty() == false) && (pinnedDocs != null && pinnedDocs.isEmpty() == false)) {
            throw new IllegalArgumentException("Can't have both pinned ids and pinned docs");
        }

        if (pinnedIds != null && pinnedIds.isEmpty() == false) {
            PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, pinnedIds.toArray(new String[0]));
            return pinnedQueryBuilder.toQuery(context);
        } else if (pinnedDocs != null && pinnedDocs.isEmpty() == false) {
            PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, pinnedDocs.toArray(new Item[0]));
            return pinnedQueryBuilder.toQuery(context);
        } else {
            return organicQuery.toQuery(context);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (pinnedIds != null || pinnedDocs != null) {
            return this;
        } else if (pinnedIdsSupplier != null || pinnedDocsSupplier != null) {
            List<String> identifiedPinnedIds = pinnedIdsSupplier != null ? pinnedIdsSupplier.get() : null;
            List<Item> identifiedPinnedDocs = pinnedDocsSupplier != null ? pinnedDocsSupplier.get() : null;
            if (identifiedPinnedIds == null && identifiedPinnedDocs == null) {
                return this; // not executed yet
            } else {
                return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds, identifiedPinnedIds, identifiedPinnedDocs, null, null);
            }
        }

        // Identify matching rules and apply them as applicable
        String rulesetId = rulesetIds.get(0);
        GetRequest getRequest = new GetRequest(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME, rulesetId);
        SetOnce<List<String>> pinnedIdsSetOnce = new SetOnce<>();
        SetOnce<List<Item>> pinnedDocsSetOnce = new SetOnce<>();
        List<String> matchingPinnedIds = new ArrayList<>();
        List<Item> matchingPinnedDocs = new ArrayList<>();

        queryRewriteContext.registerAsyncAction((client, listener) -> {
            client.get(getRequest, listener.delegateFailureAndWrap((l, getResponse) -> {
                if (getResponse.isExists() == false) {
                    throw new ResourceNotFoundException("query ruleset " + rulesetId + " not found");
                }
                QueryRuleset queryRuleset = QueryRuleset.fromXContentBytes(rulesetId, getResponse.getSourceAsBytesRef(), XContentType.JSON);
                for (QueryRule rule : queryRuleset.rules()) {
                    if (rule.type() == QueryRule.QueryRuleType.PINNED) {
                        for (QueryRuleCriteria criterion : rule.criteria()) {
                            for (String match : matchCriteria.keySet()) {
                                if (criterion.criteriaMetadata().equals(match)
                                    && criterion.criteriaType() == CriteriaType.EXACT
                                    && criterion.criteriaValue().equals(matchCriteria.get(match))) {

                                    if (rule.actions().containsKey(PinnedQueryBuilder.IDS_FIELD.getPreferredName())) {
                                        matchingPinnedIds.addAll(
                                            (List<String>) rule.actions().get(PinnedQueryBuilder.IDS_FIELD.getPreferredName())
                                        );
                                    } else if (rule.actions().containsKey(PinnedQueryBuilder.DOCS_FIELD.getPreferredName())) {
                                        List<Map<String, String>> docsToPin = (List<Map<String, String>>) rule.actions()
                                            .get(PinnedQueryBuilder.DOCS_FIELD.getPreferredName());
                                        List<Item> items = docsToPin.stream()
                                            .map(
                                                map -> new Item(
                                                    map.get(Item.INDEX_FIELD.getPreferredName()),
                                                    map.get(Item.ID_FIELD.getPreferredName())
                                                )
                                            )
                                            .toList();
                                        matchingPinnedDocs.addAll(items);
                                    } else {
                                        throw new UnsupportedOperationException("Pinned rules must specify id or docs");
                                    }
                                }
                            }
                        }
                    } else {
                        throw new IllegalStateException("unsupported query rule type [" + rule.type() + "]");
                    }
                }
                pinnedIdsSetOnce.set(matchingPinnedIds.stream().distinct().toList());
                pinnedDocsSetOnce.set(matchingPinnedDocs.stream().distinct().toList());
                listener.onResponse(null);
            }));
        });

        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        RuleQueryBuilder rewritten = new RuleQueryBuilder(
            newOrganicQuery,
            matchCriteria,
            rulesetIds,
            null,
            null,
            pinnedIdsSetOnce::get,
            pinnedDocsSetOnce::get
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
            && Objects.equals(pinnedIds, other.pinnedIds)
            && Objects.equals(pinnedDocs, other.pinnedDocs)
            && Objects.equals(pinnedIdsSupplier, other.pinnedIdsSupplier)
            && Objects.equals(pinnedDocsSupplier, other.pinnedDocsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rulesetIds, matchCriteria, organicQuery, pinnedIds, pinnedDocs, pinnedIdsSupplier, pinnedDocsSupplier);
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
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
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
