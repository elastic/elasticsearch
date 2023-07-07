/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.DOCS_FIELD;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.IDS_FIELD;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item.INDEX_FIELD;
import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.MAX_NUM_PINNED_HITS;

/**
 * A query rule consists of:
 * <ul>
 *     <li>A unique identifier</li>
 *     <li>The type of rule, e.g. pinned</li>
 *     <li>The criteria required for a query to match this rule</li>
 *     <li>The actions that should be taken if this rule is matched, dependent on the type of rule</li>
 * </ul>
 */
public class QueryRule implements Writeable, ToXContentObject {

    private final String id;
    private final QueryRuleType type;
    private final List<QueryRuleCriteria> criteria;
    private final Map<String, Object> actions;

    public enum QueryRuleType {
        PINNED;

        public static QueryRuleType queryRuleType(String type) {
            for (QueryRuleType queryRuleType : QueryRuleType.values()) {
                if (queryRuleType.name().equalsIgnoreCase(type)) {
                    return queryRuleType;
                }
            }
            throw new IllegalArgumentException("Unknown QueryRuleType: " + type);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Public constructor.
     *
     * @param id                        The unique identifier associated with this query rule
     * @param type                      The {@link QueryRuleType} of this rule
     * @param criteria                  The {@link QueryRuleCriteria} required for a query to match this rule
     * @param actions                   The actions that should be taken if this rule is matched, dependent on the type of rule
     */
    public QueryRule(String id, QueryRuleType type, List<QueryRuleCriteria> criteria, Map<String, Object> actions) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Query rule id cannot be null or blank");
        }
        this.id = id;

        Objects.requireNonNull(type, "Query rule type cannot be null");
        this.type = type;

        Objects.requireNonNull(criteria, "Query rule criteria cannot be null");
        if (criteria.isEmpty()) {
            throw new IllegalArgumentException("Query rule criteria cannot be empty");
        }
        this.criteria = criteria;

        Objects.requireNonNull(actions, "Query rule actions cannot be null");
        if (actions.isEmpty()) {
            throw new IllegalArgumentException("Query rule actions cannot be empty");
        }
        this.actions = actions;

        validate();
    }

    public QueryRule(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = QueryRuleType.queryRuleType(in.readString());
        this.criteria = in.readList(QueryRuleCriteria::new);
        this.actions = in.readMap();

        validate();
    }

    private void validate() {
        if (type == QueryRuleType.PINNED) {
            if (actions.containsKey(IDS_FIELD.getPreferredName())
                && actions.containsKey(DOCS_FIELD.getPreferredName())) {
                throw new ElasticsearchParseException("pinned query rule actions must contain only one of either ids or docs");
            } else if (actions.containsKey(IDS_FIELD.getPreferredName()) == false
                && actions.containsKey(DOCS_FIELD.getPreferredName()) == false) {
                throw new ElasticsearchParseException("pinned query rule actions must contain either ids or docs");
            } else if (actions.containsKey(IDS_FIELD.getPreferredName())) {
                validateAction(actions.get(IDS_FIELD.getPreferredName()));
            } else if (actions.containsKey(DOCS_FIELD.getPreferredName())) {
                validateAction(actions.get(DOCS_FIELD.getPreferredName()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported QueryRuleType: " + type);
        }
    }

    private void validateAction(Object action) {
        if (action instanceof List == false) {
            throw new ElasticsearchParseException("pinned query rule actions must be a list");
        } else if (((List<?>) action).isEmpty()) {
            throw new ElasticsearchParseException("pinned query rule actions cannot be empty");
        } else if (((List<?>) action).size() > MAX_NUM_PINNED_HITS) {
            throw new ElasticsearchParseException("pinned hits cannot exceed " + MAX_NUM_PINNED_HITS);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(type.toString());
        out.writeList(criteria);
        out.writeGenericMap(actions);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<QueryRule, String> PARSER = new ConstructingObjectParser<>(
        "query_rule",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            final QueryRuleType type = QueryRuleType.queryRuleType((String) params[1]);
            final List<QueryRuleCriteria> criteria = (List<QueryRuleCriteria>) params[2];
            final Map<String, Object> actions = (Map<String, Object>) params[3];
            return new QueryRule(id, type, criteria, actions);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("rule_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField CRITERIA_FIELD = new ParseField("criteria");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");

    static {
        PARSER.declareStringOrNull(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> QueryRuleCriteria.fromXContent(p), CRITERIA_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), ACTIONS_FIELD);
    }

    /**
     * Parses a {@link QueryRule} from its {@param xContentType} representation in bytes.
     *
     * @param source The bytes that represents the {@link QueryRule}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return QueryRule.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses a {@link QueryRule} through the provided {@param parser}.
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Converts the {@link QueryRule} to XContent.
     *
     * @return The {@link XContentBuilder} containing the serialized {@link QueryRule}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.xContentList(CRITERIA_FIELD.getPreferredName(), criteria);
            builder.field(ACTIONS_FIELD.getPreferredName());
            builder.map(actions);

        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns the unique ID of the {@link QueryRule}.
     *
     * @return The unique ID of the {@link QueryRule}.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the {@link QueryRuleType} of {@link QueryRule}.
     *
     * @return The type of the {@link QueryRule}.
     */
    public QueryRuleType type() {
        return type;
    }

    /**
     * Returns the {@link QueryRuleCriteria} that causes the {@link QueryRule} to match a query.
     *
     * @return the {@link QueryRuleCriteria}
     */
    public List<QueryRuleCriteria> criteria() {
        return criteria;
    }

    /**
     * Returns the actions that are executed when the {@link QueryRule} matches a query.
     *
     * @return The actions that are executed when the {@link QueryRule} matches a query.
     */
    public Map<String, Object> actions() {
        return actions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRule queryRule = (QueryRule) o;
        return Objects.equals(id, queryRule.id)
            && type == queryRule.type
            && Objects.equals(criteria, queryRule.criteria)
            && Objects.equals(actions, queryRule.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, criteria, actions);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @SuppressWarnings("unchecked")
    public AppliedQueryRules applyRule(AppliedQueryRules appliedRules, Map<String, Object> matchCriteria) {
        if (type != QueryRule.QueryRuleType.PINNED) {
            throw new UnsupportedOperationException("Only pinned query rules are supported");
        }

        List<String> matchingPinnedIds = new ArrayList<>();
        List<PinnedQueryBuilder.Item> matchingPinnedDocs = new ArrayList<>();

        for (QueryRuleCriteria criterion : criteria) {
            for (String match : matchCriteria.keySet()) {
                final String matchValue = matchCriteria.get(match).toString();
                if (criterion.criteriaMetadata().equals(match) && criterion.isMatch(matchValue)) {
                    if (actions.containsKey(IDS_FIELD.getPreferredName())) {
                        matchingPinnedIds.addAll((List<String>) actions.get(IDS_FIELD.getPreferredName()));
                    } else if (actions.containsKey(DOCS_FIELD.getPreferredName())) {
                        List<Map<String, String>> docsToPin = (List<Map<String, String>>) actions.get(DOCS_FIELD.getPreferredName());
                        List<PinnedQueryBuilder.Item> items = docsToPin.stream()
                            .map(
                                map -> new PinnedQueryBuilder.Item(
                                    map.get(INDEX_FIELD.getPreferredName()),
                                    map.get(PinnedQueryBuilder.Item.ID_FIELD.getPreferredName())
                                )
                            )
                            .toList();
                        matchingPinnedDocs.addAll(items);
                    }
                }
            }
        }

        List<String> pinnedIds = appliedRules.pinnedIds();
        List<PinnedQueryBuilder.Item> pinnedDocs = appliedRules.pinnedDocs();
        pinnedIds.addAll(matchingPinnedIds);
        pinnedDocs.addAll(matchingPinnedDocs);
        return new AppliedQueryRules(pinnedIds, pinnedDocs);
    }
}
