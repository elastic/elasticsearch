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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
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
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
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
    static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    private final String rulesetId;
    private final Map<String, Object> matchCriteria;
    private final QueryBuilder organicQuery;

    private final List<String> pinnedIds;
    private final Supplier<List<String>> pinnedIdsSupplier;
    private final List<Item> pinnedDocs;
    private final Supplier<List<Item>> pinnedDocsSupplier;

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_10_X;
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, Map<String, Object> matchCriteria, String rulesetId) {
        this(organicQuery, matchCriteria, rulesetId, null, null, null, null);
    }

    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
        matchCriteria = in.readGenericMap();
        rulesetId = in.readString();
        pinnedIds = in.readOptionalStringCollectionAsList();
        pinnedIdsSupplier = null;
        pinnedDocs = in.readOptionalCollectionAsList(Item::new);
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
        if (Strings.isNullOrEmpty(rulesetId)) {
            throw new IllegalArgumentException("rulesetId must not be null or empty");
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
        // NOTE: this is old query logic, as in 8.12.2+ and 8.13.0+ we will always rewrite this query
        // into a pinned query or the organic query. This logic remains here for backwards compatibility
        // with coordinator nodes running versions 8.10.0 - 8.12.1.
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

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (pinnedIdsSupplier != null && pinnedDocsSupplier != null) {
            List<String> identifiedPinnedIds = pinnedIdsSupplier.get();
            List<Item> identifiedPinnedDocs = pinnedDocsSupplier.get();
            if (identifiedPinnedIds == null || identifiedPinnedDocs == null) {
                return this; // Not executed yet
            } else if (identifiedPinnedIds.isEmpty() && identifiedPinnedDocs.isEmpty()) {
                return organicQuery; // Nothing to pin here
            } else if (identifiedPinnedIds.isEmpty() == false && identifiedPinnedDocs.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "applied rules contain both pinned ids and pinned docs, only one of ids or docs is allowed"
                );
            } else if (identifiedPinnedIds.isEmpty() == false) {
                return new PinnedQueryBuilder(organicQuery, truncateList(identifiedPinnedIds).toArray(new String[0]));
            } else {
                return new PinnedQueryBuilder(organicQuery, truncateList(identifiedPinnedDocs).toArray(new Item[0]));
            }
        }

        // Identify matching rules and apply them as applicable
        GetRequest getRequest = new GetRequest(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME, rulesetId);
        SetOnce<List<String>> pinnedIdsSetOnce = new SetOnce<>();
        SetOnce<List<Item>> pinnedDocsSetOnce = new SetOnce<>();
        AppliedQueryRules appliedRules = new AppliedQueryRules();

        queryRewriteContext.registerAsyncAction((client, listener) -> {
            executeAsyncWithOrigin(client, ENT_SEARCH_ORIGIN, TransportGetAction.TYPE, getRequest, ActionListener.wrap(getResponse -> {

                if (getResponse.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException("query ruleset " + rulesetId + " not found"));
                    return;
                }

                QueryRuleset queryRuleset = QueryRuleset.fromXContentBytes(rulesetId, getResponse.getSourceAsBytesRef(), XContentType.JSON);
                for (QueryRule rule : queryRuleset.rules()) {
                    rule.applyRule(appliedRules, matchCriteria);
                }
                pinnedIdsSetOnce.set(appliedRules.pinnedIds().stream().distinct().toList());
                pinnedDocsSetOnce.set(appliedRules.pinnedDocs().stream().distinct().toList());
                listener.onResponse(null);

            }, listener::onFailure));
        });

        return new RuleQueryBuilder(organicQuery, matchCriteria, this.rulesetId, null, null, pinnedIdsSetOnce::get, pinnedDocsSetOnce::get)
            .boost(this.boost)
            .queryName(this.queryName);
    }

    private List<?> truncateList(List<?> input) {
        // PinnedQueryBuilder will return an error if we attempt to return more than the maximum number of
        // pinned hits. Here, we truncate matching rules rather than return an error.
        if (input.size() > MAX_NUM_PINNED_HITS) {
            HeaderWarning.addWarning("Truncating query rule pinned hits to " + MAX_NUM_PINNED_HITS + " documents");
            return input.subList(0, MAX_NUM_PINNED_HITS);
        }
        return input;
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

    public static RuleQueryBuilder fromXContent(XContentParser parser, XPackLicenseState licenseState) {
        if (QueryRulesConfig.QUERY_RULES_LICENSE_FEATURE.check(licenseState) == false) {
            throw LicenseUtils.newComplianceException(NAME);
        }
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
