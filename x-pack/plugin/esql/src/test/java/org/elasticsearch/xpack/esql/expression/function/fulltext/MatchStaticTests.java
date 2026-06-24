/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class MatchStaticTests extends ESTestCase {
    static String ZERO_TERMS_QUERY = " . . .  ";

    public void testWithDefaultOptionsWithSingleQueryTerm() {
        Match match = matchWithText("cat", null);
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(toEvaluatorFactory(match), instanceOf(MatchTextEvaluator.Factory.class));
    }

    public void testWithDefaultOptionsWithMultipleTerms() {
        Match match = matchWithText("quick fox", null);
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses(), hasSize(2));
        assertThat(booleanQuery.clauses().get(0).occur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(0).query(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) booleanQuery.clauses().get(0).query()).getTerm().text(), equalTo("quick"));
        assertThat(booleanQuery.clauses().get(1).occur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(1).query(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) booleanQuery.clauses().get(1).query()).getTerm().text(), equalTo("fox"));

        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(MatchTextEvaluator.Factory.class));

        // operator=AND produces MUST clauses instead of the default SHOULD
        BooleanQuery andQuery = (BooleanQuery) matchWithText("quick fox", Map.of("operator", "AND")).luceneQuery(new StandardAnalyzer());
        assertThat(andQuery.clauses(), hasSize(2));
        assertThat(andQuery.clauses().get(0).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermQuery) andQuery.clauses().get(0).query()).getTerm().text(), equalTo("quick"));
        assertThat(andQuery.clauses().get(1).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermQuery) andQuery.clauses().get(1).query()).getTerm().text(), equalTo("fox"));
    }

    public void testWithDefaultOptionsWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = matchWithText(ZERO_TERMS_QUERY, null);
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));

        // fuzziness does not change the zero-tokens behavior — still MatchNoDocsQuery
        assertThat(
            matchWithText(ZERO_TERMS_QUERY, Map.of("fuzziness", "1")).luceneQuery(new StandardAnalyzer()),
            instanceOf(MatchNoDocsQuery.class)
        );
    }

    public void testWithZeroTermsQueryNoneWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = matchWithText(ZERO_TERMS_QUERY, Map.of("zero_terms_query", "none"));
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    public void testWithZeroTermsQueryAllWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = matchWithText(ZERO_TERMS_QUERY, Map.of("zero_terms_query", "all"));
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchAllDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_TRUE_FACTORY.getClass()));

        // fuzziness does not change the zero-tokens behavior with zero_terms_query=all — still MatchAllDocsQuery
        assertThat(
            matchWithText(ZERO_TERMS_QUERY, Map.of("zero_terms_query", "all", "fuzziness", "1")).luceneQuery(new StandardAnalyzer()),
            instanceOf(MatchAllDocsQuery.class)
        );
    }

    public void testWithSingleTermQueryAndFuzziness() {
        Match match = matchWithText("cat", Map.of("fuzziness", "2"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));

        var fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(0));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(2));
    }

    public void testWithMultipleTermsQueryAndFuzziness() {
        Match match = matchWithText("cat dog bear", Map.of("fuzziness", "2"));
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses(), hasSize(3));
        assertThat(booleanQuery.clauses().get(0).query(), instanceOf(FuzzyQuery.class));
        assertThat(((FuzzyQuery) booleanQuery.clauses().get(0).query()).getTerm().text(), equalTo("cat"));
        assertThat(booleanQuery.clauses().get(1).query(), instanceOf(FuzzyQuery.class));
        assertThat(((FuzzyQuery) booleanQuery.clauses().get(1).query()).getTerm().text(), equalTo("dog"));
        assertThat(booleanQuery.clauses().get(1).query(), instanceOf(FuzzyQuery.class));
        assertThat(((FuzzyQuery) booleanQuery.clauses().get(2).query()).getTerm().text(), equalTo("bear"));

        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(MatchTextEvaluator.Factory.class));

        // operator=AND produces MUST FuzzyQuery clauses
        BooleanQuery andQuery = (BooleanQuery) matchWithText("cat dog bear", Map.of("fuzziness", "2", "operator", "AND")).luceneQuery(
            new StandardAnalyzer()
        );
        assertThat(andQuery.clauses(), hasSize(3));
        assertThat(andQuery.clauses().get(0).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(andQuery.clauses().get(1).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(andQuery.clauses().get(2).occur(), equalTo(BooleanClause.Occur.MUST));
    }

    public void testWithFuzzinessAutoOnMediumWord() {
        // AUTO for a 3-5 char word yields 1 edit distance
        Match match = matchWithText("cat", Map.of("fuzziness", "AUTO"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
    }

    public void testWithFuzzinessAutoOnLongWord() {
        // AUTO for a 6+ char word yields 2 edit distances
        Match match = matchWithText("search", Map.of("fuzziness", "AUTO"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("search"));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(2));
    }

    public void testWithFuzzinessAndNonZeroPrefixLength() {
        Match match = matchWithText("cat", Map.of("fuzziness", "2", "prefix_length", "2"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(2));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(2));
    }

    public void testWithFuzzinessTranspositionsDisabled() {
        Match match = matchWithText("cat", Map.of("fuzziness", "1", "fuzzy_transpositions", "false"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTranspositions(), equalTo(false));
    }

    public void testWithFuzzinessTranspositionsExplicitlyEnabled() {
        Match match = matchWithText("cat", Map.of("fuzziness", "1", "fuzzy_transpositions", "true"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTranspositions(), equalTo(true));
    }

    public void testWithFuzzinessAndConstantScoreRewrite() {
        Match match = matchWithText("cat", Map.of("fuzziness", "1", "fuzzy_rewrite", "constant_score"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getRewriteMethod(), equalTo(MultiTermQuery.CONSTANT_SCORE_REWRITE));
    }

    public void testWithFuzzinessAndConstantScoreBlendedRewrite() {
        Match match = matchWithText("cat", Map.of("fuzziness", "1", "fuzzy_rewrite", "constant_score_blended"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getRewriteMethod(), equalTo(MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE));
    }

    public void testWithFuzzinessAndMaxExpansions() {
        // max_expansions controls how many terms the query expands to; verify the option is accepted and produces a FuzzyQuery
        Match match = matchWithText("cat", Map.of("fuzziness", "1", "max_expansions", "10"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
    }

    public void testWithAllFuzzinessOptions() {
        Match match = matchWithText(
            "cat",
            Map.of(
                "fuzziness",
                "1",
                "prefix_length",
                "1",
                "max_expansions",
                "25",
                "fuzzy_transpositions",
                "false",
                "fuzzy_rewrite",
                "constant_score_boolean"
            )
        );
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
        assertThat(fuzzyQuery.getTranspositions(), equalTo(false));
        assertThat(fuzzyQuery.getRewriteMethod(), equalTo(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE));
    }

    public void testWithMinimumShouldMatch() {
        Match match = matchWithText("cat dog bear", Map.of("minimum_should_match", "2"));
        BooleanQuery query = (BooleanQuery) match.luceneQuery(new StandardAnalyzer());
        assertThat(query.clauses(), hasSize(3));
        assertThat(query.getMinimumNumberShouldMatch(), equalTo(2));
    }

    public void testToEvaluatorReturnsBytesRefEvaluator() {
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.KEYWORD, "cat")), instanceOf(MatchBytesRefEvaluator.Factory.class));
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.IP, "127.0.0.1")), instanceOf(MatchBytesRefEvaluator.Factory.class));
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.VERSION, "1.0.0")), instanceOf(MatchBytesRefEvaluator.Factory.class));
    }

    public void testToEvaluatorReturnsLongEvaluator() {
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.LONG, "1000")), instanceOf(MatchLongEvaluator.Factory.class));
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.UNSIGNED_LONG, "1000")), instanceOf(MatchLongEvaluator.Factory.class));
        assertThat(
            toEvaluatorFactory(matchForFieldType(DataType.DATETIME, "2024-01-01T00:00:00.000Z")),
            instanceOf(MatchLongEvaluator.Factory.class)
        );
        assertThat(
            toEvaluatorFactory(matchForFieldType(DataType.DATE_NANOS, "2024-01-01T00:00:00.000Z")),
            instanceOf(MatchLongEvaluator.Factory.class)
        );
    }

    public void testToEvaluatorReturnsBooleanEvaluator() {
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.BOOLEAN, "true")), instanceOf(MatchBooleanEvaluator.Factory.class));
    }

    public void testToEvaluatorReturnsDoubleEvaluator() {
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.DOUBLE, "1.5")), instanceOf(MatchDoubleEvaluator.Factory.class));
    }

    public void testToEvaluatorReturnsIntegerEvaluator() {
        assertThat(toEvaluatorFactory(matchForFieldType(DataType.INTEGER, "42")), instanceOf(MatchIntegerEvaluator.Factory.class));
    }

    private Match matchForFieldType(DataType fieldType, String query) {
        return match(fieldType, query, null);
    }

    private ExpressionEvaluator.Factory toEvaluatorFactory(Match match) {
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return null;
            }

            @Override
            public FoldContext foldCtx() {
                return null;
            }
        };
        return match.toEvaluator(toEvaluator);
    }

    private Match matchWithText(String query, Map<String, Object> optionsMap) {
        return match(DataType.TEXT, query, optionsMap);
    }

    private Match match(DataType fieldType, String query, Map<String, Object> optionsMap) {
        Settings settings = Settings.builder().put(QueryPragmas.RUNTIME_LEXICAL_SEARCH.getKey(), true).build();
        Configuration config = EsqlTestUtils.configuration(new QueryPragmas(settings));

        MapExpression options = null;
        if (optionsMap != null) {
            List<Expression> expressions = new ArrayList<>();
            optionsMap.entrySet().forEach(entry -> {
                expressions.add(Literal.keyword(Source.EMPTY, entry.getKey()));
                expressions.add(Literal.keyword(Source.EMPTY, entry.getValue().toString()));
            });
            options = new MapExpression(Source.EMPTY, expressions);
        }

        return new Match(
            Source.EMPTY,
            new ReferenceAttribute(Source.EMPTY, "field", fieldType),
            Literal.keyword(Source.EMPTY, query),
            options,
            config
        );
    }
}
