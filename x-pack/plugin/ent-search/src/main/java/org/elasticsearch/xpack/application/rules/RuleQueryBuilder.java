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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
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
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.MAX_NUM_PINNED_HITS;

/**
 * A query that will determine based on query context and configured query rules,
 * whether a query should be modified based on actions specified in matching rules.
 *
 * This iteration will determine if a query should have pinned documents and if so,
 * modify the query accordingly to pin those documents.
 */
public class RuleQueryBuilder extends AbstractQueryBuilder<RuleQueryBuilder> {

    public static final String NAME = "rule_query";

    private static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");
    private static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    /**
     * Defines the set of allowed match criteria, so that we can validate that rule query requests are sending in allowed/supported data.
     */
    static final Set<String> ALLOWED_MATCH_CRITERIA = Set.of("query_string");

    private final String rulesetId;
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

    public RuleQueryBuilder(QueryBuilder organicQuery, Map<String, Object> matchCriteria, String rulesetId) {
        this(organicQuery, matchCriteria, rulesetId, null, null, null, null);
    }

    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
        matchCriteria = in.readMap();
        rulesetId = in.readString();
        pinnedIds = in.readOptionalStringList();
        pinnedIdsSupplier = null;
        pinnedDocs = in.readOptionalList(Item::new);
        pinnedDocsSupplier = null;
    }

    private RuleQueryBuilder(
        QueryBuilder organicQuery,
        Map<String, Object> matchCriteria,
        String rulesetId,
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
        if (Strings.isNullOrEmpty(rulesetId)) {
            throw new IllegalArgumentException("rulesetId must not be null or empty");
        }

        // PinnedQueryBuilder will return an error if we attmept to return more than the maximum number of
        // pinned hits. Here, we truncate matching rules rather than return an error.
        if (pinnedIds != null && pinnedIds.size() > MAX_NUM_PINNED_HITS) {
            pinnedIds = pinnedIds.subList(0, MAX_NUM_PINNED_HITS);
        }

        if (pinnedDocs != null && pinnedDocs.size() > MAX_NUM_PINNED_HITS) {
            pinnedDocs = pinnedDocs.subList(0, MAX_NUM_PINNED_HITS);
        }

        this.organicQuery = organicQuery;
        this.matchCriteria = matchCriteria;
        this.rulesetId = rulesetId;
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
        if (curatedDocsSupplier != null) {
            throw new IllegalStateException("curatedDocsSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }

        out.writeNamedWriteable(organicQuery);
        out.writeGenericMap(matchCriteria);
        out.writeString(rulesetId);
        out.writeOptionalStringCollection(pinnedIds);
        out.writeOptionalCollection(pinnedDocs);
    }

    public String rulesetId() {
        return rulesetId;
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
        builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if ((pinnedIds != null && pinnedIds.isEmpty() == false) && (pinnedDocs != null && pinnedDocs.isEmpty() == false)) {
            throw new IllegalArgumentException("applied rules contain both pinned ids and pinned docs, only one of ids or docs is allowed");
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
                return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetId, identifiedPinnedIds, identifiedPinnedDocs, null, null);
            }
        }

        // Identify matching rules and apply them as applicable
        GetRequest getRequest = new GetRequest(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME, rulesetId);
        SetOnce<List<String>> pinnedIdsSetOnce = new SetOnce<>();
        SetOnce<List<Item>> pinnedDocsSetOnce = new SetOnce<>();
        AppliedQueryRules appliedRules = new AppliedQueryRules();

        queryRewriteContext.registerAsyncAction((client, listener) -> {
            Client clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
            clientWithOrigin.get(getRequest, listener.delegateFailureAndWrap((l, getResponse) -> {
                if (getResponse.isExists() == false) {
                    throw new ResourceNotFoundException("query ruleset " + rulesetId + " not found");
                }
                QueryRuleset queryRuleset = QueryRuleset.fromXContentBytes(rulesetId, getResponse.getSourceAsBytesRef(), XContentType.JSON);
                for (QueryRule rule : queryRuleset.rules()) {
                    logger.info("Applying rule: " + rule);
                    rule.applyRule(appliedRules, matchCriteria);
                }
                pinnedIdsSetOnce.set(appliedRules.pinnedIds().stream().distinct().toList());
                pinnedDocsSetOnce.set(appliedRules.pinnedDocs().stream().distinct().toList());
                listener.onResponse(null);
            }));
        });

        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        RuleQueryBuilder rewritten = new RuleQueryBuilder(
            newOrganicQuery,
            matchCriteria,
            this.rulesetId,
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
        return Objects.equals(rulesetId, other.rulesetId)
            && Objects.equals(matchCriteria, other.matchCriteria)
            && Objects.equals(organicQuery, other.organicQuery)
            && Objects.equals(pinnedIds, other.pinnedIds)
            && Objects.equals(pinnedDocs, other.pinnedDocs)
            && Objects.equals(pinnedIdsSupplier, other.pinnedIdsSupplier)
            && Objects.equals(pinnedDocsSupplier, other.pinnedDocsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rulesetId, matchCriteria, organicQuery, pinnedIds, pinnedDocs, pinnedIdsSupplier, pinnedDocsSupplier);
    }

    private static final ConstructingObjectParser<RuleQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        QueryBuilder organicQuery = (QueryBuilder) a[0];
        @SuppressWarnings("unchecked")
        Map<String, Object> matchCriteria = (Map<String, Object>) a[1];
        String rulesetId = (String) a[2];
        return new RuleQueryBuilder(organicQuery, matchCriteria, rulesetId);
    });
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
        PARSER.declareString(constructorArg(), RULESET_ID_FIELD);
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
