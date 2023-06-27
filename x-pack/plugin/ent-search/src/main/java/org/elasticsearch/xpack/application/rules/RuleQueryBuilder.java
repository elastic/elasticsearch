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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

/**
 * A query that will promote selected documents (identified by ID) above matches produced by an "organic" query. In practice, some upstream
 * system will identify the promotions associated with a user's query string and use this object to ensure these are "pinned" to the top of
 * the other search results.
 */
public class RuleQueryBuilder extends AbstractQueryBuilder<RuleQueryBuilder> {
    public static final String NAME = "rule_query";

    private static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    private static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    private static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");
    private static final ParseField CURATED_IDS_FIELD = new ParseField("curated_ids");
    private static final ParseField CURATED_DOCS_FIELD = new ParseField("curated_docs");
    private final List<String> rulesetIds;
    private final Map<String,Object> matchCriteria;
    private QueryBuilder organicQuery;
    private final List<String> curatedIds;
    private final Supplier<List<String>> curatedIdSupplier;
    private final List<Item> curatedDocs;
    private final Supplier<List<Item>> curatedDocsSupplier;

    private final Logger logger = LogManager.getLogger(RuleQueryBuilder.class);

    public RuleQueryBuilder(QueryBuilder organicQuery, List<String> rulesetIds, Map<String,Object> matchCriteria) {
        this(organicQuery, rulesetIds, matchCriteria, null, null, null, null);
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, List<String> rulesetIds, Map<String,Object> matchCriteria, Supplier<List<String>> curatedIdSupplier, Supplier<List<Item>> curatedDocsSupplier) {
        this(organicQuery, rulesetIds, matchCriteria, null, curatedIdSupplier, null, curatedDocsSupplier);
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, List<String> rulesetIds, Map<String,Object> matchCriteria, List<String> curatedIds, List<Item> curatedDocs) {
        this(organicQuery, rulesetIds, matchCriteria, curatedIds, null, curatedDocs, null);
    }

    public RuleQueryBuilder(QueryBuilder organicQuery, List<String> rulesetIds, Map<String,Object> matchCriteria, List<String> curatedIds, Supplier<List<String>> curatedIdSupplier, List<Item> curatedDocs, Supplier<List<Item>> curatedDocsSupplier) {
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
        this.curatedIds = curatedIds;
        this.curatedIdSupplier = curatedIdSupplier;
        this.curatedDocs = curatedDocs;
        this.curatedDocsSupplier = curatedDocsSupplier;
    }

    /**
     * Read from a stream.
     */
    public RuleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.organicQuery = in.readNamedWriteable(QueryBuilder.class);
        this.rulesetIds = in.readOptionalStringList();
        this.matchCriteria = in.readMap();
        curatedIds = in.readOptionalStringList();
        curatedIdSupplier = null;
        curatedDocs = in.readBoolean() ? in.readList(Item::new) : null;
        curatedDocsSupplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(organicQuery);
        out.writeOptionalStringCollection(rulesetIds);
        out.writeGenericMap(matchCriteria);
        out.writeOptionalStringCollection(curatedIds);
        if (curatedDocs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(curatedDocs);
        }
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
        if (organicQuery != null) {
            builder.field(ORGANIC_QUERY_FIELD.getPreferredName());
            organicQuery.toXContent(builder, params);
        }

        if (rulesetIds != null) {
            builder.startArray(RULESET_IDS_FIELD.getPreferredName());
            for (String value : rulesetIds) {
                builder.value(value);
            }
            builder.endArray();
        }

        if (matchCriteria != null) {
            builder.field(MATCH_CRITERIA_FIELD.getPreferredName());
            builder.map(matchCriteria);
        }

        if (curatedIds != null) {
            builder.startArray(CURATED_IDS_FIELD.getPreferredName());
            for (String value : curatedIds) {
                builder.value(value);
            }
            builder.endArray();
        }

        if (curatedDocs != null) {
            builder.startArray(CURATED_DOCS_FIELD.getPreferredName());
            for (Item value : curatedDocs) {
                value.toXContent(builder, params);
            }
            builder.endArray();
        }

        boostAndQueryNameToXContent(builder);
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

    @SuppressWarnings("unchecked")
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (curatedIds != null && (curatedIds.isEmpty() == false) || (curatedDocs != null && curatedDocs.isEmpty() == false)) {
            // We have at least one of curated IDs and curated docs. We're done.
            return this;
        } else if (curatedIdSupplier != null) {
            List<String> curatedIds = curatedIdSupplier.get();
            if (curatedIds == null) {
                return this; // not executed yet
            } else {
                // We have curated IDs. We're done.
                return new RuleQueryBuilder(organicQuery, rulesetIds, matchCriteria, curatedIds, curatedDocs);
            }
        } else if (curatedDocsSupplier != null) {
            List<Item> curatedDocs = curatedDocsSupplier.get();
            if (curatedDocs == null) {
                return this; // not executed yet
            } else {
                // We have curated docs. We're done.
                return new RuleQueryBuilder(organicQuery, rulesetIds, matchCriteria, curatedIds, curatedDocs);
            }
        }

        // Identify matching rules and apply them if applicable
        SetOnce<List<String>> idSetOnce = new SetOnce<>();
        SetOnce<List<Item>> docsSetOnce = new SetOnce<>();
        List<String> pinnedIds = new ArrayList<>();
        List<Item> pinnedDocs = new ArrayList<>();
        for (String rulesetId : rulesetIds) {
            GetQueryRulesetAction.Request getQueryRulesetRequest = new GetQueryRulesetAction.Request(rulesetId);
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                client.execute(GetQueryRulesetAction.INSTANCE, getQueryRulesetRequest, ActionListener.wrap(getQueryRulesetResponse -> {
                    QueryRuleset queryRuleset = getQueryRulesetResponse.queryRuleset();

                    for (QueryRule rule : queryRuleset.rules()) {
                        // TODO check criteria to see if the rule matches.

                        // If rule matches, add the pinned IDs/docs.
                        pinnedIds.addAll((List<String>) rule.actions().get("ids"));
                        pinnedDocs.addAll((List<Item>) rule.actions().get("docs"));
                    }
                    listener.onResponse(null);
                }, listener::onFailure));
            });
        }
        idSetOnce.set(pinnedIds.stream().distinct().toList());
        docsSetOnce.set(pinnedDocs.stream().distinct().toList());

        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        RuleQueryBuilder rewritten =
            new RuleQueryBuilder(newOrganicQuery, rulesetIds, matchCriteria, curatedIds, idSetOnce::get, curatedDocs, docsSetOnce::get);
        rewritten.boost(this.boost);
        return rewritten;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder;
        if (curatedIds != null) {
            pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, curatedIds.toArray(new String[0]));
        } else if (curatedDocs != null) {
            pinnedQueryBuilder = new PinnedQueryBuilder(organicQuery, curatedDocs.toArray(new Item[0]));
        } else {
            throw new IllegalArgumentException("Either curatedIds or curatedDocs must be set");
        }

        return pinnedQueryBuilder.toQuery(context);
    }

    @Override
    public boolean doEquals(RuleQueryBuilder other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (super.equals(other) == false) return false;
        return Objects.equals(rulesetIds, other.rulesetIds) && Objects.equals(matchCriteria, other.matchCriteria) && Objects.equals(
            organicQuery,
            other.organicQuery
        ) && Objects.equals(curatedIds, other.curatedIds) && Objects.equals(curatedIdSupplier, other.curatedIdSupplier) && Objects.equals(
            curatedDocs,
            other.curatedDocs
        ) && Objects.equals(curatedDocsSupplier, other.curatedDocsSupplier) && Objects.equals(logger, other.logger);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.hashCode(),
            rulesetIds,
            matchCriteria,
            organicQuery,
            curatedIds,
            curatedIdSupplier,
            curatedDocs,
            curatedDocsSupplier,
            logger
        );
    }

    // TODO update this to 8.10.0
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_9_0;
    }
}
