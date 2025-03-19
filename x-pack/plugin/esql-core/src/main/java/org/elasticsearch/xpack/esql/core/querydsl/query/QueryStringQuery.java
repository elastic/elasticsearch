/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

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

public class QueryStringQuery extends Query {

    private static final Map<String, BiConsumer<QueryStringQueryBuilder, Object>> BUILDER_APPLIERS = Map.ofEntries(
        entry(ALLOW_LEADING_WILDCARD_FIELD.getPreferredName(), (qb, obj) -> qb.allowLeadingWildcard((Boolean) obj)),
        entry(ANALYZE_WILDCARD_FIELD.getPreferredName(), (qb, obj) -> qb.analyzeWildcard((Boolean) obj)),
        entry(ANALYZER_FIELD.getPreferredName(), (qb, obj) -> qb.analyzer((String) obj)),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), (qb, obj) -> qb.autoGenerateSynonymsPhraseQuery((Boolean) obj)),
        entry(DEFAULT_FIELD_FIELD.getPreferredName(), (qb, obj) -> qb.defaultField((String) obj)),
        entry(DEFAULT_OPERATOR_FIELD.getPreferredName(), (qb, obj) -> qb.defaultOperator(Operator.fromString((String) obj))),
        entry(ENABLE_POSITION_INCREMENTS_FIELD.getPreferredName(), (qb, obj) -> qb.enablePositionIncrements((Boolean) obj)),
        entry(ESCAPE_FIELD.getPreferredName(), (qb, obj) -> qb.escape((Boolean) obj)),
        entry(FUZZINESS_FIELD.getPreferredName(), (qb, obj) -> qb.fuzziness(Fuzziness.fromString((String) obj))),
        entry(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyMaxExpansions((Integer) obj)),
        entry(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyPrefixLength((Integer) obj)),
        entry(FUZZY_REWRITE_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyRewrite((String) obj)),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyTranspositions((Boolean) obj)),
        entry(LENIENT_FIELD.getPreferredName(), (qb, obj) -> qb.lenient((Boolean) obj)),
        entry(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), (qb, obj) -> qb.maxDeterminizedStates((Integer) obj)),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), (qb, obj) -> qb.minimumShouldMatch((String) obj)),
        entry(PHRASE_SLOP_FIELD.getPreferredName(), (qb, obj) -> qb.phraseSlop((Integer) obj)),
        entry(REWRITE_FIELD.getPreferredName(), (qb, obj) -> qb.rewrite((String) obj)),
        entry(QUOTE_ANALYZER_FIELD.getPreferredName(), (qb, obj) -> qb.quoteAnalyzer((String) obj)),
        entry(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), (qb, obj) -> qb.quoteFieldSuffix((String) obj)),
        entry(TIE_BREAKER_FIELD.getPreferredName(), (qb, obj) -> qb.tieBreaker((Float) obj)),
        entry(TIME_ZONE_FIELD.getPreferredName(), (qb, obj) -> qb.timeZone((String) obj)),
        entry(
            TYPE_FIELD.getPreferredName(),
            (qb, obj) -> qb.type(MultiMatchQueryBuilder.Type.parse((String) obj, LoggingDeprecationHandler.INSTANCE))
        )
    );

    private final String query;
    private final Map<String, Float> fields;
    private final Map<String, Object> options;

    public QueryStringQuery(Source source, String query, Map<String, Float> fields, Map<String, Object> options) {
        super(source);
        this.query = query;
        this.fields = fields;
        this.options = options == null ? Collections.emptyMap() : options;
    }

    @Override
    protected QueryBuilder asBuilder() {
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
        return Objects.hash(query, fields);
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
        return Objects.equals(query, other.query) && Objects.equals(fields, other.fields) && Objects.equals(options, other.options);
    }

    @Override
    protected String innerToString() {
        return fields + ":" + query;
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
