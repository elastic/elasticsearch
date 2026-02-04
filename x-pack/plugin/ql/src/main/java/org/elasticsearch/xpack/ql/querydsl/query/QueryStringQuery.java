/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ALLOW_LEADING_WILDCARD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ANALYZE_WILDCARD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.DEFAULT_FIELD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.DEFAULT_OPERATOR_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ENABLE_POSITION_INCREMENTS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ESCAPE_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZINESS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.MAX_DETERMINIZED_STATES_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.PHRASE_SLOP_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_FIELD_SUFFIX_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.REWRITE_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.TIE_BREAKER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.TIME_ZONE_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.TYPE_FIELD;

public class QueryStringQuery extends LeafQuery {

    private static final Map<String, BiConsumer<QueryStringQueryBuilder, String>> BUILDER_APPLIERS = Map.ofEntries(
        entry(ALLOW_LEADING_WILDCARD_FIELD.getPreferredName(), (qb, s) -> qb.allowLeadingWildcard(Booleans.parseBoolean(s))),
        entry(ANALYZE_WILDCARD_FIELD.getPreferredName(), (qb, s) -> qb.analyzeWildcard(Booleans.parseBoolean(s))),
        entry(ANALYZER_FIELD.getPreferredName(), QueryStringQueryBuilder::analyzer),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), (qb, s) -> qb.autoGenerateSynonymsPhraseQuery(Booleans.parseBoolean(s))),
        entry(DEFAULT_FIELD_FIELD.getPreferredName(), QueryStringQueryBuilder::defaultField),
        entry(DEFAULT_OPERATOR_FIELD.getPreferredName(), (qb, s) -> qb.defaultOperator(Operator.fromString(s))),
        entry(ENABLE_POSITION_INCREMENTS_FIELD.getPreferredName(), (qb, s) -> qb.enablePositionIncrements(Booleans.parseBoolean(s))),
        entry(ESCAPE_FIELD.getPreferredName(), (qb, s) -> qb.escape(Booleans.parseBoolean(s))),
        entry(FUZZINESS_FIELD.getPreferredName(), (qb, s) -> qb.fuzziness(Fuzziness.fromString(s))),
        entry(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), (qb, s) -> qb.fuzzyMaxExpansions(Integer.parseInt(s))),
        entry(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), (qb, s) -> qb.fuzzyPrefixLength(Integer.parseInt(s))),
        entry(FUZZY_REWRITE_FIELD.getPreferredName(), QueryStringQueryBuilder::fuzzyRewrite),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), (qb, s) -> qb.fuzzyTranspositions(Booleans.parseBoolean(s))),
        entry(LENIENT_FIELD.getPreferredName(), (qb, s) -> qb.lenient(Booleans.parseBoolean(s))),
        entry(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), (qb, s) -> qb.maxDeterminizedStates(Integer.parseInt(s))),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), QueryStringQueryBuilder::minimumShouldMatch),
        entry(PHRASE_SLOP_FIELD.getPreferredName(), (qb, s) -> qb.phraseSlop(Integer.parseInt(s))),
        entry(REWRITE_FIELD.getPreferredName(), QueryStringQueryBuilder::rewrite),
        entry(QUOTE_ANALYZER_FIELD.getPreferredName(), QueryStringQueryBuilder::quoteAnalyzer),
        entry(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), QueryStringQueryBuilder::quoteFieldSuffix),
        entry(TIE_BREAKER_FIELD.getPreferredName(), (qb, s) -> qb.tieBreaker(Float.parseFloat(s))),
        entry(TIME_ZONE_FIELD.getPreferredName(), QueryStringQueryBuilder::timeZone),
        entry(TYPE_FIELD.getPreferredName(), (qb, s) -> qb.type(MultiMatchQueryBuilder.Type.parse(s, LoggingDeprecationHandler.INSTANCE)))
    );

    private final String query;
    private final Map<String, Float> fields;
    private final StringQueryPredicate predicate;
    private final Map<String, String> options;

    // dedicated constructor for QueryTranslator
    public QueryStringQuery(Source source, String query, String fieldName) {
        this(source, query, Collections.singletonMap(fieldName, 1.0f), null);
    }

    public QueryStringQuery(Source source, String query, Map<String, Float> fields, StringQueryPredicate predicate) {
        super(source);
        this.query = query;
        this.fields = fields;
        this.predicate = predicate;
        this.options = predicate == null ? Collections.emptyMap() : predicate.optionMap();
    }

    @Override
    public QueryBuilder asBuilder() {
        final QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query);
        queryBuilder.fields(fields);
        options.forEach((k, v) -> {
            if (BUILDER_APPLIERS.containsKey(k)) {
                BUILDER_APPLIERS.get(k).accept(queryBuilder, v);
            } else {
                throw new IllegalArgumentException("illegal query_string option [" + k + "]");
            }
        });
        return queryBuilder;
    }

    public Map<String, Float> fields() {
        return fields;
    }

    public String query() {
        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, fields, predicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryStringQuery other = (QueryStringQuery) obj;
        return Objects.equals(query, other.query) && Objects.equals(fields, other.fields) && Objects.equals(predicate, other.predicate);
    }

    @Override
    protected String innerToString() {
        return fields + ":" + query;
    }
}
