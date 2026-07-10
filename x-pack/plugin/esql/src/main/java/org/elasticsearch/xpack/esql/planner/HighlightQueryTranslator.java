/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Translates a {@code HIGHLIGHT} query expression into a Lucene {@link Query}. */
public final class HighlightQueryTranslator {

    private static final String BOOST_OPTION = AbstractQueryBuilder.BOOST_FIELD.getPreferredName();
    private static final String OPERATOR_OPTION = MatchQueryBuilder.OPERATOR_FIELD.getPreferredName();
    private static final String FUZZINESS_OPTION = Fuzziness.FIELD.getPreferredName();
    private static final String PREFIX_LENGTH_OPTION = MatchQueryBuilder.PREFIX_LENGTH_FIELD.getPreferredName();
    private static final String MAX_EXPANSIONS_OPTION = MatchQueryBuilder.MAX_EXPANSIONS_FIELD.getPreferredName();
    private static final String FUZZY_TRANSPOSITIONS_OPTION = MatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD.getPreferredName();
    private static final String MINIMUM_SHOULD_MATCH_OPTION = MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD.getPreferredName();
    private static final String SLOP_OPTION = MatchPhraseQueryBuilder.SLOP_FIELD.getPreferredName();
    private static final String DEFAULT_FIELD_OPTION = QueryStringQueryBuilder.DEFAULT_FIELD_FIELD.getPreferredName();
    private static final String EMPTY_QUERY_REASON = "HIGHLIGHT query is empty";
    private static final String NO_TERMS_REASON = "HIGHLIGHT query produced no terms";
    // TODO: Widen QSTR support beyond [default_field] to the query_string options that map onto the classic
    // Lucene QueryParser without needing an analyzer/mapping/SearchExecutionContext, wired in translateQueryString the same
    // way MATCH/MATCH_PHRASE options are (read the option map, configure the parser/post-process the query):
    // - parser setters: default_operator, allow_leading_wildcard, analyze_wildcard, enable_position_increments, phrase_slop,
    // fuzzy_prefix_length, max_determinized_states (setDeterminizeWorkLimit), rewrite (setMultiTermRewriteMethod via a
    // QueryParsers.parseRewriteMethod-style helper)
    // - parser overrides (no classic setter exists): fuzziness (default getFuzzyDistance for a bare '~'), fuzzy_max_expansions
    // and fuzzy_transpositions (override newFuzzyQuery)
    // - post-process the parsed query: boost (reuse applyBoost) and minimum_should_match (Queries.maybeApplyMinimumShouldMatch
    // on the top-level boolean)
    // These options are rejected until HIGHLIGHT has a real analyzer/mapping: analyzer, quote_analyzer, quote_field_suffix,
    // auto_generate_synonyms_phrase_query, lenient, time_zone.
    private static final Set<String> QUERY_STRING_ALLOWED_OPTIONS = Set.of(DEFAULT_FIELD_OPTION);

    // TODO: support the [analyzer] option once HIGHLIGHT can use non-default analyzers.
    private static final Set<String> MATCH_REJECTED_OPTIONS = Set.of(
        MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(),
        MatchQueryBuilder.FUZZY_REWRITE_FIELD.getPreferredName(),
        MatchQueryBuilder.ZERO_TERMS_QUERY_FIELD.getPreferredName(),
        MatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(),
        MatchQueryBuilder.LENIENT_FIELD.getPreferredName()
    );
    private static final Set<String> MATCH_PHRASE_REJECTED_OPTIONS = Set.of(
        MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(),
        MatchPhraseQueryBuilder.ZERO_TERMS_QUERY_FIELD.getPreferredName()
    );

    private final List<String> fields;
    private final Analyzer defaultAnalyzer;

    private HighlightQueryTranslator(List<String> fields, Analyzer defaultAnalyzer) {
        this.fields = fields;
        this.defaultAnalyzer = defaultAnalyzer;
    }

    /**
     * Translates {@code query} into a single Lucene {@link Query} over the given {@code fields}.
     * <p>
     * A query that folds to a string is parsed with {@code query_string} semantics; anything else is translated as an
     * expression tree.
     *
     * @param query            the resolved HIGHLIGHT query expression (a full-text function, a boolean combination of
     *                         them, or an expression that folds to a query string)
     * @param fields           the real {@code ON} field names, in order
     * @param defaultAnalyzer  the analyzer used to tokenize the query text
     * @throws IllegalArgumentException when the expression, a function, or an option is not supported by HIGHLIGHT
     */
    public static Query translate(Expression query, List<String> fields, Analyzer defaultAnalyzer) {
        String literal = queryTextIfLiteral(query);
        if (literal != null) {
            return translateLiteral(literal, fields, defaultAnalyzer);
        }
        return new HighlightQueryTranslator(fields, defaultAnalyzer).doTranslate(query);
    }

    /** Parses a literal query string over the {@code ON} fields using query_string semantics. */
    public static Query translateLiteral(String queryText, List<String> fields, Analyzer analyzer) {
        return parseQueryString(queryStringParser(fields, analyzer), queryText);
    }

    /** Folded string query text, or {@code null} when the query does not fold to a string. */
    public static String queryTextIfLiteral(Expression query) {
        if (query.foldable() == false) {
            return null;
        }
        Object folded = query.fold(FoldContext.small());
        return folded instanceof BytesRef || folded instanceof String ? BytesRefs.toString(folded) : null;
    }

    private Query doTranslate(Expression expr) {
        // MatchOperator (':') extends Match.
        if (expr instanceof Match match) {
            return translateMatch(match);
        }
        if (expr instanceof MatchPhrase matchPhrase) {
            return translateMatchPhrase(matchPhrase);
        }
        if (expr instanceof QueryString queryString) {
            return translateQueryString(queryString);
        }
        if (expr instanceof And and) {
            return translateBoolean(and.left(), and.right(), BooleanClause.Occur.MUST);
        }
        if (expr instanceof Or or) {
            return translateBoolean(or.left(), or.right(), BooleanClause.Occur.SHOULD);
        }
        if (expr instanceof Not not) {
            return Queries.not(doTranslate(not.field()));
        }
        if (expr instanceof Kql) {
            throw new IllegalArgumentException("HIGHLIGHT does not support [KQL] queries yet");
        }
        if (expr instanceof Literal literal && DataType.isString(literal.dataType())) {
            return translateLiteral(BytesRefs.toString(literal.value()), fields, defaultAnalyzer);
        }
        throw new IllegalArgumentException(
            "HIGHLIGHT query must be a full-text function (MATCH, MATCH_PHRASE, QSTR) or a boolean combination of them, found ["
                + expr.sourceText()
                + "]"
        );
    }

    private Query translateBoolean(Expression left, Expression right, BooleanClause.Occur occur) {
        return new BooleanQuery.Builder().add(doTranslate(left), occur).add(doTranslate(right), occur).build();
    }

    private Query translateMatch(Match match) {
        Map<String, Object> options = optionMap(match.options(), match.source(), Match.ALLOWED_OPTIONS);
        rejectBlockedOptions(options, MATCH_REJECTED_OPTIONS, "MATCH");
        String field = fieldName(match.field());
        requireOnField(field);
        String text = queryText(match.query());
        Query query = createMatchQueryBuilder(options, defaultAnalyzer).createBooleanQuery(field, text, matchOperator(options));
        query = matchNoTermsAsNoDocs(query, "HIGHLIGHT MATCH produced no terms");
        query = Queries.maybeApplyMinimumShouldMatch(query, stringOption(options, MINIMUM_SHOULD_MATCH_OPTION));
        return applyBoost(query, options);
    }

    private Query translateMatchPhrase(MatchPhrase matchPhrase) {
        Map<String, Object> options = optionMap(matchPhrase.options(), matchPhrase.source(), MatchPhrase.ALLOWED_OPTIONS);
        rejectBlockedOptions(options, MATCH_PHRASE_REJECTED_OPTIONS, "MATCH_PHRASE");
        String field = fieldName(matchPhrase.field());
        requireOnField(field);
        String text = queryText(matchPhrase.query());
        int slop = intOption(options, SLOP_OPTION, 0);

        Query query = new QueryBuilder(defaultAnalyzer).createPhraseQuery(field, text, slop);
        query = matchNoTermsAsNoDocs(query, "HIGHLIGHT MATCH_PHRASE produced no terms");
        return applyBoost(query, options);
    }

    private Query translateQueryString(QueryString queryString) {
        Map<String, Object> options = optionMap(queryString.options(), queryString.source(), QueryString.ALLOWED_OPTIONS);
        rejectOptionsNotIn(options, QUERY_STRING_ALLOWED_OPTIONS, "QSTR");
        String text = queryText(queryString.query());
        String defaultField = stringOption(options, DEFAULT_FIELD_OPTION);
        if (defaultField != null) {
            requireOnField(defaultField);
        }
        List<String> targetFields = defaultField != null ? List.of(defaultField) : fields;
        return translateLiteral(text, targetFields, defaultAnalyzer);
    }

    private static Query applyBoost(Query query, Map<String, Object> options) {
        Float boost = floatOption(options, BOOST_OPTION);
        return boost == null ? query : new BoostQuery(query, boost);
    }

    /** Rejects any option in {@code blocked} that is present (a blocklist). */
    private static void rejectBlockedOptions(Map<String, Object> options, Set<String> blocked, String functionName) {
        for (String name : blocked) {
            if (options.containsKey(name)) {
                throw new IllegalArgumentException("HIGHLIGHT does not support the [" + name + "] option of [" + functionName + "]");
            }
        }
    }

    /** Rejects any present option that is not in {@code supported} (an allowlist). */
    private static void rejectOptionsNotIn(Map<String, Object> options, Set<String> supported, String functionName) {
        for (String name : options.keySet()) {
            if (supported.contains(name) == false) {
                throw new IllegalArgumentException("HIGHLIGHT does not support the [" + name + "] option of [" + functionName + "]");
            }
        }
    }

    private static Map<String, Object> optionMap(Expression options, Source source, Map<String, DataType> allowedOptions) {
        if (options instanceof MapExpression mapExpression) {
            Map<String, Object> converted = new HashMap<>();
            Options.populateMap(mapExpression, converted, source, TypeResolutions.ParamOrdinal.SECOND, allowedOptions);
            return converted;
        }
        return Map.of();
    }

    private static String fieldName(Expression field) {
        return field instanceof NamedExpression named ? named.name() : Expressions.name(field);
    }

    /**
     * HIGHLIGHT indexes only the {@code ON} fields with {@code require_field_match=true}, so a MATCH/MATCH_PHRASE
     * target or QSTR {@code default_field} outside {@code ON} can never highlight. Fail instead of returning null.
     * Field-qualified literal query strings are intentionally left alone.
     */
    private void requireOnField(String field) {
        if (fields.contains(field) == false) {
            throw new IllegalArgumentException("HIGHLIGHT query field [" + field + "] is not in ON fields " + fields);
        }
    }

    private static String queryText(Expression query) {
        return BytesRefs.toString(query.fold(FoldContext.small()));
    }

    @Nullable
    private static String stringOption(Map<String, Object> options, String name) {
        Object value = options.get(name);
        return value == null ? null : value.toString();
    }

    @Nullable
    private static Float floatOption(Map<String, Object> options, String name) {
        return (Float) options.get(name);
    }

    private static int intOption(Map<String, Object> options, String name, int defaultValue) {
        Object value = options.get(name);
        return value == null ? defaultValue : (Integer) value;
    }

    private static boolean boolOption(Map<String, Object> options, String name, boolean defaultValue) {
        Object value = options.get(name);
        return value == null ? defaultValue : (Boolean) value;
    }

    private static Query matchNoTermsAsNoDocs(@Nullable Query query, String reason) {
        return query == null ? Queries.newMatchNoDocsQuery(reason) : query;
    }

    /**
     * Resolves the MATCH {@code operator} option to a boolean occur, matching Query DSL semantics: a missing option
     * defaults to {@code OR}, {@code and}/{@code or} are accepted case-insensitively, and any other value is rejected.
     */
    private static BooleanClause.Occur matchOperator(Map<String, Object> options) {
        String operator = stringOption(options, OPERATOR_OPTION);
        if (operator == null) {
            return BooleanClause.Occur.SHOULD;
        }
        try {
            return Operator.fromString(operator).toBooleanClauseOccur();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "HIGHLIGHT MATCH [" + OPERATOR_OPTION + "] must be one of [OR, AND], found [" + operator + "]"
            );
        }
    }

    private static QueryBuilder createMatchQueryBuilder(Map<String, Object> options, Analyzer analyzer) {
        String fuzzinessValue = stringOption(options, FUZZINESS_OPTION);
        if (fuzzinessValue == null) {
            return new QueryBuilder(analyzer);
        }
        return new FuzzyQueryBuilder(
            analyzer,
            Fuzziness.fromString(fuzzinessValue),
            intOption(options, PREFIX_LENGTH_OPTION, FuzzyQuery.defaultPrefixLength),
            intOption(options, MAX_EXPANSIONS_OPTION, FuzzyQuery.defaultMaxExpansions),
            boolOption(options, FUZZY_TRANSPOSITIONS_OPTION, FuzzyQuery.defaultTranspositions)
        );
    }

    private static Query parseQueryString(QueryParser parser, String queryText) {
        if (queryText == null || queryText.isBlank()) {
            return Queries.newMatchNoDocsQuery(EMPTY_QUERY_REASON);
        }
        try {
            Query query = parser.parse(queryText);
            if (query instanceof BooleanQuery bq && bq.clauses().isEmpty()) {
                return Queries.newMatchNoDocsQuery(NO_TERMS_REASON);
            }
            return query;
        } catch (ParseException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid query [" + queryText + "] in HIGHLIGHT: " + e.getMessage(), e);
        }
    }

    /** Builds a query_string parser already configured with the ES {@code query_string} defaults. */
    private static QueryParser queryStringParser(List<String> fields, Analyzer analyzer) {
        QueryParser parser = switch (fields.size()) {
            case 1 -> new HighlightSingleFieldParser(fields.getFirst(), analyzer);
            default -> new HighlightMultiFieldParser(fields.toArray(String[]::new), analyzer);
        };
        parser.setAllowLeadingWildcard(true); // ES query_string default (Lucene defaults to false)
        parser.setDefaultOperator(QueryParser.Operator.OR);
        return parser;
    }

    /** Matches Query DSL query_string fuzzy-distance semantics ({@link Fuzziness#AUTO} for bare {@code ~}). */
    private static float queryStringFuzzyDistance(Token fuzzyToken, String termStr) {
        if (fuzzyToken.image.length() == 1) {
            return Fuzziness.AUTO.asDistance(termStr);
        }
        return Fuzziness.fromString(fuzzyToken.image.substring(1)).asDistance(termStr);
    }

    private static final class HighlightSingleFieldParser extends QueryParser {
        HighlightSingleFieldParser(String field, Analyzer analyzer) {
            super(field, analyzer);
        }

        @Override
        protected float getFuzzyDistance(Token fuzzyToken, String termStr) {
            return queryStringFuzzyDistance(fuzzyToken, termStr);
        }

        /**
         * Keep regexp parsing case-sensitive to match Query DSL {@code query_string}.
         * <p>
         * The classic {@link QueryParser} normalizes regexp text through {@link Analyzer#normalize}, but DSL regexp
         * queries keep the pattern as-is ({@code StringFieldType#regexpQuery} does not normalize it). Without this
         * override, an uppercase pattern like {@code /M(ount|t)/} would incorrectly match the lowercased term
         * {@code mount}.
         */
        @Override
        protected Query getRegexpQuery(String field, String termStr) {
            return newRegexpQuery(new Term(field, termStr));
        }
    }

    private static final class HighlightMultiFieldParser extends MultiFieldQueryParser {
        HighlightMultiFieldParser(String[] fields, Analyzer analyzer) {
            super(fields, analyzer);
        }

        @Override
        protected float getFuzzyDistance(Token fuzzyToken, String termStr) {
            return queryStringFuzzyDistance(fuzzyToken, termStr);
        }

        /**
         * Same case-sensitive regexp behavior as {@link HighlightSingleFieldParser#getRegexpQuery}, while preserving
         * {@link MultiFieldQueryParser}'s multi-field fan-out.
         */
        @Override
        protected Query getRegexpQuery(String field, String termStr) throws ParseException {
            // When the query has no explicit field, fan it out across all parser fields.
            // We do not use per-field boosts here, so applying boosts would be a no-op.
            if (field == null) {
                List<Query> clauses = new ArrayList<>(fields.length);
                for (String f : fields) {
                    clauses.add(getRegexpQuery(f, termStr));
                }
                return getMultiFieldQuery(clauses);
            }
            return newRegexpQuery(new Term(field, termStr));
        }
    }

    private static final class FuzzyQueryBuilder extends QueryBuilder {
        private final Fuzziness fuzziness;
        private final int prefixLength;
        private final int maxExpansions;
        private final boolean transpositions;

        FuzzyQueryBuilder(Analyzer analyzer, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            super(analyzer);
            this.fuzziness = fuzziness;
            this.prefixLength = prefixLength;
            this.maxExpansions = maxExpansions;
            this.transpositions = transpositions;
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            return new FuzzyQuery(term, fuzziness.asDistance(term.text()), prefixLength, maxExpansions, transpositions);
        }
    }
}
