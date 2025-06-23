/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.PREFIX_LENGTH_FIELD;

public class MatchQuery extends Query {

    private static final Map<String, BiConsumer<MatchQueryBuilder, Object>> BUILDER_APPLIERS;

    static {
        // TODO: add zero terms query support, I'm not sure the best way to parse it yet...
        // appliers.put("zero_terms_query", (qb, s) -> qb.zeroTermsQuery(s));
        BUILDER_APPLIERS = Map.ofEntries(
            entry(ANALYZER_FIELD.getPreferredName(), (qb, s) -> qb.analyzer(s.toString())),
            entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), (qb, b) -> qb.autoGenerateSynonymsPhraseQuery((Boolean) b)),
            entry(Fuzziness.FIELD.getPreferredName(), (qb, s) -> qb.fuzziness(Fuzziness.fromString(s.toString()))),
            entry(AbstractQueryBuilder.BOOST_FIELD.getPreferredName(), (qb, s) -> qb.boost((Float) s)),
            entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), (qb, s) -> qb.fuzzyTranspositions((Boolean) s)),
            entry(FUZZY_REWRITE_FIELD.getPreferredName(), (qb, s) -> qb.fuzzyRewrite(s.toString())),
            entry(MatchQueryBuilder.LENIENT_FIELD.getPreferredName(), (qb, s) -> qb.lenient((Boolean) s)),
            entry(MAX_EXPANSIONS_FIELD.getPreferredName(), (qb, s) -> qb.maxExpansions((Integer) s)),
            entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), (qb, s) -> qb.minimumShouldMatch(s.toString())),
            entry(OPERATOR_FIELD.getPreferredName(), (qb, s) -> qb.operator(Operator.fromString(s.toString()))),
            entry(PREFIX_LENGTH_FIELD.getPreferredName(), (qb, s) -> qb.prefixLength((Integer) s))
        );
    }

    private final String name;
    private final Object text;
    private final Double boost;
    private final Fuzziness fuzziness;
    private final Map<String, Object> options;

    public MatchQuery(Source source, String name, Object text) {
        this(source, name, text, Map.of());
    }

    public MatchQuery(Source source, String name, Object text, Map<String, Object> options) {
        super(source);
        assert options != null;
        this.name = name;
        this.text = text;
        this.options = options;
        this.boost = null;
        this.fuzziness = null;
    }

    @Override
    protected QueryBuilder asBuilder() {
        final MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(name, text);
        options.forEach((k, v) -> {
            if (BUILDER_APPLIERS.containsKey(k)) {
                BUILDER_APPLIERS.get(k).accept(queryBuilder, v);
            } else {
                throw new IllegalArgumentException("illegal match option [" + k + "]");
            }
        });
        if (boost != null) {
            queryBuilder.boost(boost.floatValue());
        }
        if (fuzziness != null) {
            queryBuilder.fuzziness(fuzziness);
        }
        return queryBuilder;
    }

    public String name() {
        return name;
    }

    public Object text() {
        return text;
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, name, options, boost, fuzziness);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        MatchQuery other = (MatchQuery) obj;
        return Objects.equals(text, other.text)
            && Objects.equals(name, other.name)
            && Objects.equals(options, other.options)
            && Objects.equals(boost, other.boost)
            && Objects.equals(fuzziness, other.fuzziness);
    }

    @Override
    protected String innerToString() {
        return name + ":" + text;
    }

    public Map<String, Object> options() {
        return options;
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
