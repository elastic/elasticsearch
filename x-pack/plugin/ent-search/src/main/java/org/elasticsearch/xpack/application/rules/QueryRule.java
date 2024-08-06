/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.searchbusinessrules.SpecifiedDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.ALWAYS;

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

    public static final int MAX_NUM_DOCS_IN_RULE = 100;
    public static final ParseField IDS_FIELD = new ParseField("ids");
    public static final ParseField DOCS_FIELD = new ParseField("docs");
    public static final ParseField INDEX_FIELD = new ParseField("_index");

    private final String id;
    private final QueryRuleType type;
    private final List<QueryRuleCriteria> criteria;
    private final Map<String, Object> actions;
    private final Integer priority;

    public static final int MIN_PRIORITY = 0;
    public static final int MAX_PRIORITY = 1000000;

    public enum QueryRuleType {
        EXCLUDE,
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
     * @param priority        If specified, assigns a priority to the rule. Rules with specified priorities are applied before
     *                                  rules without specified priorities, in ascending priority order.
     */
    public QueryRule(
        @Nullable String id,
        QueryRuleType type,
        List<QueryRuleCriteria> criteria,
        Map<String, Object> actions,
        @Nullable Integer priority
    ) {
        // Interstitial null state allowed during rule creation; validation occurs in CRUD API
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
        this.priority = priority;

        validate();
    }

    public QueryRule(String id, QueryRule other) {
        this(id, other.type, other.criteria, other.actions, other.priority);
    }

    public QueryRule(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = QueryRuleType.queryRuleType(in.readString());
        this.criteria = in.readCollectionAsList(QueryRuleCriteria::new);
        this.actions = in.readGenericMap();

        if (in.getTransportVersion().onOrAfter(TransportVersions.QUERY_RULE_CRUD_API_PUT)) {
            this.priority = in.readOptionalVInt();
        } else {
            this.priority = null;
        }

        validate();
    }

    private void validate() {

        if (priority != null && (priority < MIN_PRIORITY || priority > MAX_PRIORITY)) {
            throw new IllegalArgumentException("Priority was " + priority + ", must be between " + MIN_PRIORITY + " and " + MAX_PRIORITY);
        }

        if (Set.of(QueryRuleType.PINNED, QueryRuleType.EXCLUDE).contains(type)) {
            boolean ruleContainsIds = actions.containsKey(IDS_FIELD.getPreferredName());
            boolean ruleContainsDocs = actions.containsKey(DOCS_FIELD.getPreferredName());
            if (ruleContainsIds ^ ruleContainsDocs) {
                validateIdOrDocAction(actions.get(IDS_FIELD.getPreferredName()));
                validateIdOrDocAction(actions.get(DOCS_FIELD.getPreferredName()));
            } else {
                throw new ElasticsearchParseException(type.toString() + " query rule actions must contain only one of either ids or docs");
            }
        }
    }

    private void validateIdOrDocAction(Object action) {
        if (action != null) {
            if (action instanceof List == false) {
                throw new ElasticsearchParseException(type + " query rule actions must be a list");
            } else if (((List<?>) action).isEmpty()) {
                throw new ElasticsearchParseException(type + " query rule actions cannot be empty");
            } else if (((List<?>) action).size() > MAX_NUM_DOCS_IN_RULE) {
                throw new ElasticsearchParseException(type + " documents cannot exceed " + MAX_NUM_DOCS_IN_RULE);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(type.toString());
        out.writeCollection(criteria);
        out.writeGenericMap(actions);
        if (out.getTransportVersion().onOrAfter(TransportVersions.QUERY_RULE_CRUD_API_PUT)) {
            out.writeOptionalVInt(priority);
        }
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
            final Integer priority = (Integer) params[4];
            return new QueryRule(id, type, criteria, actions, priority);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("rule_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField CRITERIA_FIELD = new ParseField("criteria");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");
    public static final ParseField PRIORITY_FIELD = new ParseField("priority");

    static {
        PARSER.declareStringOrNull(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> QueryRuleCriteria.fromXContent(p), CRITERIA_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), ACTIONS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), PRIORITY_FIELD);
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
            if (priority != null) {
                builder.field(PRIORITY_FIELD.getPreferredName(), priority);
            }
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

    public Integer priority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRule queryRule = (QueryRule) o;
        return Objects.equals(id, queryRule.id)
            && type == queryRule.type
            && Objects.equals(criteria, queryRule.criteria)
            && Objects.equals(actions, queryRule.actions)
            && Objects.equals(priority, queryRule.priority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, criteria, actions, priority);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public AppliedQueryRules applyRule(AppliedQueryRules appliedRules, Map<String, Object> matchCriteria) {
        List<SpecifiedDocument> pinnedDocs = appliedRules.pinnedDocs();
        List<SpecifiedDocument> excludedDocs = appliedRules.excludedDocs();
        List<SpecifiedDocument> matchingDocs = identifyMatchingDocs(matchCriteria);

        switch (type) {
            case PINNED -> pinnedDocs.addAll(matchingDocs);
            case EXCLUDE -> excludedDocs.addAll(matchingDocs);
            default -> throw new IllegalStateException("Unsupported query rule type: " + type);
        }
        return new AppliedQueryRules(pinnedDocs, excludedDocs);
    }

    @SuppressWarnings("unchecked")
    private List<SpecifiedDocument> identifyMatchingDocs(Map<String, Object> matchCriteria) {
        List<SpecifiedDocument> matchingDocs = new ArrayList<>();
        Boolean isRuleMatch = null;

        // All specified criteria in a rule must match for the rule to be applied
        for (QueryRuleCriteria criterion : criteria) {
            for (String match : matchCriteria.keySet()) {
                final Object matchValue = matchCriteria.get(match);
                final QueryRuleCriteriaType criteriaType = criterion.criteriaType();
                final String criteriaMetadata = criterion.criteriaMetadata();

                if (criteriaType == ALWAYS || (criteriaMetadata != null && criteriaMetadata.equals(match))) {
                    boolean singleCriterionMatches = criterion.isMatch(matchValue, criteriaType, false);
                    isRuleMatch = (isRuleMatch == null) ? singleCriterionMatches : isRuleMatch && singleCriterionMatches;
                }
            }
        }

        if (isRuleMatch != null && isRuleMatch) {
            if (actions.containsKey(IDS_FIELD.getPreferredName())) {
                matchingDocs.addAll(
                    ((List<String>) actions.get(IDS_FIELD.getPreferredName())).stream().map(id -> new SpecifiedDocument(null, id)).toList()
                );
            } else if (actions.containsKey(DOCS_FIELD.getPreferredName())) {
                List<Map<String, String>> docsToPin = (List<Map<String, String>>) actions.get(DOCS_FIELD.getPreferredName());
                List<SpecifiedDocument> specifiedDocuments = docsToPin.stream()
                    .map(
                        map -> new SpecifiedDocument(
                            map.get(INDEX_FIELD.getPreferredName()),
                            map.get(SpecifiedDocument.ID_FIELD.getPreferredName())
                        )
                    )
                    .toList();
                matchingDocs.addAll(specifiedDocuments);
            }
        }
        return matchingDocs;
    }

}
