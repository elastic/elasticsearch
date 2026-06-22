/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
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
    public void testWithDefaultOptionsWithSingleQueryTerm() {
        Match match = match("cat", null);
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(TermQuery.class));
    }

    public void testWithDefaultOptionsWithMultipleTerms() {
        Match match = match("quick fox", null);
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses(), hasSize(2));
        assertThat(booleanQuery.clauses().get(0).query(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) booleanQuery.clauses().get(0).query()).getTerm().text(), equalTo("quick"));
        assertThat(booleanQuery.clauses().get(1).query(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) booleanQuery.clauses().get(1).query()).getTerm().text(), equalTo("fox"));

        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(MatchTextEvaluator.Factory.class));
    }

    public void testWithDefaultOptionsWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = match(" . . .  ", null);
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    public void testWithZeroTermsQueryNoneWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = match(" . . .  ", Map.of("zero_terms_query", "none"));
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    public void testWithZeroTermsQueryAllWithNoQueryTerms() {
        // we pass a query string that has no analyzed tokens
        Match match = match(" . . .  ", Map.of("zero_terms_query", "all"));
        var query = match.luceneQuery(new StandardAnalyzer());

        assertThat(query, instanceOf(MatchAllDocsQuery.class));
        ExpressionEvaluator.Factory evaluatorFactor = toEvaluatorFactory(match);
        assertThat(evaluatorFactor, instanceOf(ConstantEvaluators.CONSTANT_TRUE_FACTORY.getClass()));
    }

    public void testWithSingleTermQueryAndFuzziness() {
        Match match = match("cat", Map.of("fuzziness", "2"));
        var query = match.luceneQuery(new StandardAnalyzer());
        assertThat(query, instanceOf(FuzzyQuery.class));

        var fuzzyQuery = (FuzzyQuery) query;
        assertThat(fuzzyQuery.getTerm().text(), equalTo("cat"));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(0));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(2));
    }

    public void testWithMultipleTermsQueryAndFuzziness() {
        Match match = match("cat dog bear", Map.of("fuzziness", "2"));
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

    private Match match(String query, Map<String, Object> optionsMap) {
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
            new ReferenceAttribute(Source.EMPTY, "field", DataType.TEXT),
            Literal.text(Source.EMPTY, query),
            options,
            config
        );
    }
}
