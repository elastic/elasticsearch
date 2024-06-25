/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.Limit;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.EsqlAggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.search.Rank;

import java.time.Period;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

//
// Tests for Search commands
//
public class SearchStatementParserTests extends AbstractStatementParserTests {

    public void testSearchIndexPattern() {
        assumeTrue("search command requires snapshot build", Build.current().isSnapshot());

        assertStatement(
            "SEARCH index [ ]",
            new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "index"), List.of(), IndexMode.STANDARD)
        );
        assertStatement(
            "SEARCH foo,bar [ ]",
            new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "foo,bar"), List.of(), IndexMode.STANDARD)
        );
        assertStatement(
            "SEARCH foo*,bar [ ]",
            new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "foo*,bar"), List.of(), IndexMode.STANDARD)
        );
        assertStatement(
            "SEARCH foo-*,bar [ ]",
            new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "foo-*,bar"), List.of(), IndexMode.STANDARD)
        );
        assertStatement(
            "SEARCH foo-*,bar+* [ ]",
            new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "foo-*,bar+*"), List.of(), IndexMode.STANDARD)
        );
    }

    public void testSearchFilter() {
        assumeTrue("search command requires snapshot build", Build.current().isSnapshot());

        assertStatement("""
            SEARCH index [
              | WHERE a > 1
            ]
            """, filter(esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD), greaterThan(attribute("a"), integer(1))));

        assertStatement(
            """
                SEARCH index [
                  | WHERE a > 1 ]
                | keep emp_no
                """,
            new Keep(
                EMPTY,
                filter(esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD), greaterThan(attribute("a"), integer(1))),
                namedExpression("emp_no")
            )
        );

        assertStatement(
            """
                SEARCH index [
                  | WHERE a > 1 ]
                | limit 101
                """,
            limit(
                integer(101),
                filter(esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD), greaterThan(attribute("a"), integer(1)))
            )
        );
    }

    public void testSearchRank() {
        assumeTrue("search command requires snapshot build", Build.current().isSnapshot());

        assertStatement("""
            SEARCH index [
              | RANK a > 1 ]
            """, rank(esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD), greaterThan(attribute("a"), integer(1))));

        assertStatement(
            """
                SEARCH index [
                  | RANK MATCH(item, "iphone red")
                  | LIMIT 100 ]
                """,
            limit(
                integer(100),
                rank(esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD), matchQueryPredicate("item", "iphone red"))
            )
        );

        assertStatement(
            """
                SEARCH index [
                  | RANK MATCH(item, "iphone") AND MATCH(color, "red") ]
                """,
            rank(
                esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD),
                and(matchQueryPredicate("item", "iphone"), matchQueryPredicate("color", "red"))
            )
        );

        assertStatement(
            """
                SEARCH index [
                  | RANK MATCH(country, "mexico") OR MATCH(country, "spain") ]
                """,
            rank(
                esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD),
                or(matchQueryPredicate("country", "mexico"), matchQueryPredicate("country", "spain"))
            )
        );

        assertStatement(
            """
                SEARCH index [
                  | RANK MATCH(pet, "dog") AND (MATCH(color, "brown") OR MATCH(color, "red")) ]
                """,
            rank(
                esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD),
                and(matchQueryPredicate("pet", "dog"), or(matchQueryPredicate("color", "brown"), matchQueryPredicate("color", "red")))
            )
        );

        assertStatement(
            """
                SEARCH index [
                  | RANK (MATCH(color, "brown") OR MATCH(color, "red")) AND MATCH(pet, "dog") ]
                """,
            rank(
                esqlUnresolvedRelation("index", List.of(), IndexMode.STANDARD),
                and(or(matchQueryPredicate("color", "brown"), matchQueryPredicate("color", "red")), matchQueryPredicate("pet", "dog"))
            )
        );
    }

    public void testSearchExamples() {
        assumeTrue("search command requires snapshot build", Build.current().isSnapshot());

        // This example is taken from the 8.14 blog, without the knn (for now)
        assertStatement(
            """
                SEARCH images [
                  | WHERE date > now() - 1 month
                  | RANK MATCH(scene, "mountain lake")
                  | WHERE _score > 0.1
                  | LIMIT 100
                ]
                | STATS c = COUNT(votes) BY rating
                | LIMIT 5
                """,
            limit(
                integer(5),
                new EsqlAggregate(
                    EMPTY,
                    limit(
                        integer(100),
                        filter(
                            rank(
                                filter(
                                    esqlUnresolvedRelation("images", List.of(), IndexMode.STANDARD),
                                    greaterThan(
                                        attribute("date"),
                                        sub(new UnresolvedFunction(EMPTY, "now", DEFAULT, List.of()), literalDatePeriod(Period.of(0, 1, 0)))
                                    )
                                ),
                                matchQueryPredicate("scene", "mountain lake")
                            ),
                            greaterThan(attribute("_score"), literalDouble(0.1))
                        )
                    ),
                    List.of(attribute("rating")),
                    List.of(
                        new Alias(EMPTY, "c", new UnresolvedFunction(EMPTY, "COUNT", DEFAULT, List.of(attribute("votes")))),
                        attribute("rating")
                    )
                )
            )
        );
    }

    public void testSearchErrors() {
        assumeTrue("search command requires snapshot build", Build.current().isSnapshot());

        expectError("""
            SEARCH index [
              | WHERE a > 1
              | LIMIT -1 ]
            """, "extraneous input '-' expecting INTEGER_LITERAL");

        expectError("""
            SEARCH index [
              | WHERE a > 1 ]
            | LIMIT -1
            """, "extraneous input '-' expecting INTEGER_LITERAL");

        // TODO: additional negative / expected error tests go here
    }

    static Literal literalDatePeriod(Period period) {
        return new Literal(EMPTY, period, DataType.DATE_PERIOD);
    }

    static List<NamedExpression> namedExpression(String name) {
        return List.of(attribute(name));
    }

    static Filter filter(LogicalPlan child, Expression condition) {
        return new Filter(EMPTY, child, condition);
    }

    static Limit limit(Expression limit, LogicalPlan child) {
        return new Limit(EMPTY, limit, child);
    }

    static Rank rank(LogicalPlan child, Expression query) {
        return new Rank(EMPTY, child, query);
    }

    static EsqlUnresolvedRelation esqlUnresolvedRelation(String index, List<Attribute> metadataFields, IndexMode indexMode) {
        return new EsqlUnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, index), metadataFields, indexMode);
    }

    static MatchQueryPredicate matchQueryPredicate(String attr, String query) {
        return new MatchQueryPredicate(EMPTY, attribute(attr), query, null);
    }

    static GreaterThan greaterThan(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right);
    }

    static Sub sub(Expression left, Expression right) {
        return new Sub(EMPTY, left, right);
    }

    static And and(Expression left, Expression right) {
        return new And(EMPTY, left, right);
    }

    static Or or(Expression left, Expression right) {
        return new Or(EMPTY, left, right);
    }
}
