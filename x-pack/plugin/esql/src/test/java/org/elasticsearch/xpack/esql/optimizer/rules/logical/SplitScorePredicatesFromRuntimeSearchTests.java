/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class SplitScorePredicatesFromRuntimeSearchTests extends ESTestCase {

    // ... | eval content = <text> | where match(content, "fox") and _score > 1.5
    // => ... | eval content = <text> | where match(content, "fox") | where _score > 1.5
    public void testScorePredicateSplitsOutAboveRuntimeSearch() {
        MetadataAttribute score = scoreAttribute();
        Eval eval = runtimeTextEval(relation(List.of(score)));
        Match match = runtimeMatch(eval);
        GreaterThan scoreCondition = greaterThanOf(score, new Literal(EMPTY, 1.5, DataType.DOUBLE));
        Filter filter = new Filter(EMPTY, eval, new And(EMPTY, scoreCondition, match));

        LogicalPlan expected = new Filter(EMPTY, new Filter(EMPTY, eval, match), scoreCondition);
        assertEquals(expected, new SplitScorePredicatesFromRuntimeSearch().apply(filter));
    }

    // ... | where match(content, "fox") and _score > 1.5 and b < 2
    // => ... | where match(content, "fox") and b < 2 | where _score > 1.5
    public void testOnlyScorePredicatesSplitOut() {
        MetadataAttribute score = scoreAttribute();
        FieldAttribute b = getFieldAttribute("b");
        Eval eval = runtimeTextEval(relation(List.of(b, score)));
        Match match = runtimeMatch(eval);
        GreaterThan scoreCondition = greaterThanOf(score, new Literal(EMPTY, 1.5, DataType.DOUBLE));
        LessThan conditionB = lessThanOf(b, TWO);
        Filter filter = new Filter(EMPTY, eval, new And(EMPTY, new And(EMPTY, match, scoreCondition), conditionB));

        LogicalPlan expected = new Filter(EMPTY, new Filter(EMPTY, eval, new And(EMPTY, match, conditionB)), scoreCondition);
        assertEquals(expected, new SplitScorePredicatesFromRuntimeSearch().apply(filter));
    }

    // ... | where match(content, "fox") or _score > 1.5 => unchanged
    public void testScorePredicateOredWithRuntimeSearchStaysTogether() {
        MetadataAttribute score = scoreAttribute();
        Eval eval = runtimeTextEval(relation(List.of(score)));
        Filter filter = new Filter(
            EMPTY,
            eval,
            new Or(EMPTY, runtimeMatch(eval), greaterThanOf(score, new Literal(EMPTY, 1.5, DataType.DOUBLE)))
        );

        assertEquals(filter, new SplitScorePredicatesFromRuntimeSearch().apply(filter));
    }

    // ... | eval s = _score, content = <text> | where match(content, "fox") and s < 0.5 => unchanged
    public void testCopyOfScoreStaysWithRuntimeSearch() {
        MetadataAttribute score = scoreAttribute();
        EsRelation relation = relation(List.of(score));
        Alias copy = new Alias(EMPTY, "s", score);
        Eval eval = new Eval(EMPTY, relation, List.of(runtimeTextAlias(), copy));
        LessThan copyCondition = lessThanOf(copy.toAttribute(), new Literal(EMPTY, 0.5, DataType.DOUBLE));
        Filter filter = new Filter(EMPTY, eval, new And(EMPTY, runtimeMatch(eval), copyCondition));

        assertEquals(filter, new SplitScorePredicatesFromRuntimeSearch().apply(filter));
    }

    // ... | where match(title, "fox") and _score > 1.5 => unchanged
    public void testScorePredicateStaysWithIndexedSearch() {
        MetadataAttribute score = scoreAttribute();
        FieldAttribute title = getFieldAttribute("title", DataType.TEXT);
        Match match = new Match(EMPTY, title, Literal.keyword(EMPTY, "fox"), null);
        assertFalse(match.isRuntimeSearch());
        Filter filter = new Filter(
            EMPTY,
            relation(List.of(title, score)),
            new And(EMPTY, match, greaterThanOf(score, new Literal(EMPTY, 1.5, DataType.DOUBLE)))
        );

        assertEquals(filter, new SplitScorePredicatesFromRuntimeSearch().apply(filter));
    }

    private static MetadataAttribute scoreAttribute() {
        return new MetadataAttribute(EMPTY, MetadataAttribute.SCORE, DataType.DOUBLE, false);
    }

    private static EsRelation relation(List<Attribute> fieldAttributes) {
        return new EsRelation(
            EMPTY,
            randomIdentifier(),
            randomFrom(IndexMode.availableModes()),
            Map.of(),
            Map.of(),
            Map.of(),
            fieldAttributes
        );
    }

    // Not a plain rename, so a search on it is a runtime one.
    private static Alias runtimeTextAlias() {
        return new Alias(EMPTY, "content", new Literal(EMPTY, new BytesRef("quick fox"), DataType.TEXT));
    }

    private static Eval runtimeTextEval(LogicalPlan child) {
        return new Eval(EMPTY, child, List.of(runtimeTextAlias()));
    }

    private static Match runtimeMatch(Eval eval) {
        Match match = new Match(EMPTY, eval.fields().get(0).toAttribute(), Literal.keyword(EMPTY, "fox"), null);
        assertTrue(match.isRuntimeSearch());
        return match;
    }
}
