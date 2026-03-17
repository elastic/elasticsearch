/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePatternList;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class ParamsParserTests extends AbstractStatementParserTests {

    public void testDoubleParamsForIdentifier() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        // There are three variations of double parameters - named, positional or anonymous, e.g. ??n, ??1 or ??, covered.
        // Each query is executed three times with the three variations.

        // field names can appear in eval/where/stats/sort/keep/drop/rename/dissect/grok/enrich/mvexpand
        // eval, where
        List<List<String>> doubleParams = new ArrayList<>(3);
        List<String> namedDoubleParams = List.of("??f0", "??fn1", "??f1", "??f2", "??f3");
        List<String> positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5");
        List<String> anonymousDoubleParams = List.of("??", "??", "??", "??", "??");
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | eval {} = {}({})
                | where {} == {}
                | limit 1""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEqualsIgnoringIds(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1.")))))),
                        new Equals(EMPTY, attribute("f.2"), attribute("f3"))
                    )
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("f1", "f1."),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f3")
                        )
                    )
                )
            );
        }

        namedDoubleParams = List.of("??f0", "??fn1", "??f1", "??f2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | eval {} = {}({}.{})
                    | where {}.{} == {}.{}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7)
            );
            assertEqualsIgnoringIds(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(
                            EMPTY,
                            relation("test"),
                            List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1..f.2")))))
                        ),
                        new Equals(EMPTY, attribute("f3.*.f.4."), attribute("f.5.*.f.*.6"))
                    )
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("f1", "f1."),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f3.*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.*.6")
                        )
                    )
                )
            );
        }

        // stats, sort, mv_expand
        namedDoubleParams = List.of("??fn2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats y = {}({}) by {}
                | sort {}
                | mv_expand {}""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEqualsIgnoringIds(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.4.")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f3.*")))), attribute("f.4."))
                        ),
                        List.of(new Order(EMPTY, attribute("f.5.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.6*"),
                    attribute("f.6*")
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("f3", "f3.*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }

        namedDoubleParams = List.of("??fn2", "??f7", "??f8", "??f9", "??f10", "??f11", "??f12", "??f13", "??f14");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8", "??9");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | stats y = {}({}.{}) by {}.{}
                    | sort {}.{}
                    | mv_expand {}.{}""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8)
            );
            assertEqualsIgnoringIds(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.9.f10.*")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f.7*.f8.")))), attribute("f.9.f10.*"))
                        ),
                        List.of(new Order(EMPTY, attribute("f.11..f.12.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.*.13.f.14*"),
                    attribute("f.*.13.f.14*")
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("f7", "f.7*"),
                            paramAsConstant("f8", "f8."),
                            paramAsConstant("f9", "f.9"),
                            paramAsConstant("f10", "f10.*"),
                            paramAsConstant("f11", "f.11."),
                            paramAsConstant("f12", "f.12.*"),
                            paramAsConstant("f13", "f.*.13"),
                            paramAsConstant("f14", "f.14*")
                        )
                    )
                )
            );
        }

        // keep, drop, rename, grok, dissect, lookup join
        namedDoubleParams = List.of("??f1", "??f2", "??f3", "??f4", "??f5", "??f6", "??f7", "??f8", "??f9");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8", "??9");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | keep {}, {}
                    | drop {}, {}
                    | dissect {} "%{bar}"
                    | grok {} "%{WORD:foo}"
                    | rename {} as {}
                    | lookup join idx on {}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8)
            );
            LogicalPlan plan = query(
                query,
                new QueryParams(
                    List.of(
                        paramAsConstant("f1", "f.1.*"),
                        paramAsConstant("f2", "f.2"),
                        paramAsConstant("f3", "f3."),
                        paramAsConstant("f4", "f4.*"),
                        paramAsConstant("f5", "f.5*"),
                        paramAsConstant("f6", "f.6."),
                        paramAsConstant("f7", "f7*."),
                        paramAsConstant("f8", "f.8"),
                        paramAsConstant("f9", "f9")
                    )
                )
            );
            Limit limit = as(plan, Limit.class);
            LookupJoin join = as(limit.child(), LookupJoin.class);
            UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
            assertEquals(ur.indexPattern().indexPattern(), "idx");
            assertEquals(join.config().type().joinName(), "LEFT OUTER");
            assertEqualsIgnoringIds(join.config().leftFields(), List.of(attribute("f9")));
            Rename rename = as(join.left(), Rename.class);
            assertEqualsIgnoringIds(rename.renamings(), List.of(new Alias(EMPTY, "f.8", attribute("f7*."))));
            Grok grok = as(rename.child(), Grok.class);
            assertEqualsIgnoringIds(grok.input(), attribute("f.6."));
            assertEquals("%{WORD:foo}", grok.parser().pattern());
            assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
            Dissect dissect = as(grok.child(), Dissect.class);
            assertEqualsIgnoringIds(dissect.input(), attribute("f.5*"));
            assertEquals("%{bar}", dissect.parser().pattern());
            assertEquals("", dissect.parser().appendSeparator());
            assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
            Drop drop = as(dissect.child(), Drop.class);
            List<? extends NamedExpression> removals = drop.removals();
            assertEqualsIgnoringIds(removals, List.of(attribute("f3."), attribute("f4.*")));
            Keep keep = as(drop.child(), Keep.class);
            assertEqualsIgnoringIds(keep.projections(), List.of(attribute("f.1.*"), attribute("f.2")));
        }

        namedDoubleParams = List.of(
            "??f1",
            "??f2",
            "??f3",
            "??f4",
            "??f5",
            "??f6",
            "??f7",
            "??f8",
            "??f9",
            "??f10",
            "??f11",
            "??f12",
            "??f13",
            "??f14"
        );
        positionalDoubleParams = List.of(
            "??1",
            "??2",
            "??3",
            "??4",
            "??5",
            "??6",
            "??7",
            "??8",
            "??9",
            "??10",
            "??11",
            "??12",
            "??13",
            "??14"
        );
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | keep {}.{}
                    | drop {}.{}
                    | dissect {}.{} "%{bar}"
                    | grok {}.{} "%{WORD:foo}"
                    | rename {}.{} as {}.{}
                    | lookup join idx on {}.{}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8),
                params.get(9),
                params.get(10),
                params.get(11),
                params.get(12),
                params.get(13)
            );
            LogicalPlan plan = query(
                query,
                new QueryParams(
                    List.of(
                        paramAsConstant("f1", "f.1.*"),
                        paramAsConstant("f2", "f.2"),
                        paramAsConstant("f3", "f3."),
                        paramAsConstant("f4", "f4.*"),
                        paramAsConstant("f5", "f.5*"),
                        paramAsConstant("f6", "f.6."),
                        paramAsConstant("f7", "f7*."),
                        paramAsConstant("f8", "f.8"),
                        paramAsConstant("f9", "f.9*"),
                        paramAsConstant("f10", "f.10."),
                        paramAsConstant("f11", "f11*."),
                        paramAsConstant("f12", "f.12"),
                        paramAsConstant("f13", "f13"),
                        paramAsConstant("f14", "f14")
                    )
                )
            );
            Limit limit = as(plan, Limit.class);
            LookupJoin join = as(limit.child(), LookupJoin.class);
            UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
            assertEquals(ur.indexPattern().indexPattern(), "idx");
            assertEquals(join.config().type().joinName(), "LEFT OUTER");
            assertEqualsIgnoringIds(join.config().leftFields(), List.of(attribute("f13.f14")));
            Rename rename = as(join.left(), Rename.class);
            assertEqualsIgnoringIds(rename.renamings(), List.of(new Alias(EMPTY, "f11*..f.12", attribute("f.9*.f.10."))));
            Grok grok = as(rename.child(), Grok.class);
            assertEqualsIgnoringIds(grok.input(), attribute("f7*..f.8"));
            assertEquals("%{WORD:foo}", grok.parser().pattern());
            assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
            Dissect dissect = as(grok.child(), Dissect.class);
            assertEqualsIgnoringIds(dissect.input(), attribute("f.5*.f.6."));
            assertEquals("%{bar}", dissect.parser().pattern());
            assertEquals("", dissect.parser().appendSeparator());
            assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
            Drop drop = as(dissect.child(), Drop.class);
            List<? extends NamedExpression> removals = drop.removals();
            assertEqualsIgnoringIds(removals, List.of(attribute("f3..f4.*")));
            Keep keep = as(drop.child(), Keep.class);
            assertEqualsIgnoringIds(keep.projections(), List.of(attribute("f.1.*.f.2")));
        }

        // enrich, lookup join
        namedDoubleParams = List.of("??f1", "??f2", "??f3");
        positionalDoubleParams = List.of("??1", "??2", "??3");
        anonymousDoubleParams = List.of("??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                "from idx1 | ENRICH idx2 ON {} WITH {} = {}",
                params.get(0),
                params.get(1),
                params.get(2)
            );
            assertEqualsIgnoringIds(
                new Enrich(
                    EMPTY,
                    relation("idx1"),
                    null,
                    Literal.keyword(EMPTY, "idx2"),
                    attribute("f.1.*"),
                    null,
                    Map.of(),
                    List.of(new Alias(EMPTY, "f.2", attribute("f.3*")))
                ),
                query(
                    query,
                    new QueryParams(List.of(paramAsConstant("f1", "f.1.*"), paramAsConstant("f2", "f.2"), paramAsConstant("f3", "f.3*")))
                )
            );
        }

        // lookup join on expression
        namedDoubleParams = List.of("??f1", "??f2", "??f3", "??f4");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4");
        anonymousDoubleParams = List.of("??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | lookup join idx on {}.{} == {}.{}
                | limit 1""", params.get(0), params.get(1), params.get(2), params.get(3));
            LogicalPlan plan = query(
                query,
                new QueryParams(
                    List.of(
                        paramAsConstant("f1", "f.1"),
                        paramAsConstant("f2", "f.2"),
                        paramAsConstant("f3", "f.3"),
                        paramAsConstant("f4", "f.4")
                    )
                )
            );
            Limit limit = as(plan, Limit.class);
            LookupJoin join = as(limit.child(), LookupJoin.class);
            UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
            assertEquals(ur.indexPattern().indexPattern(), "idx");
            assertTrue(join.config().type().joinName().contains("LEFT OUTER"));
            EsqlBinaryComparison on = as(join.config().joinOnConditions(), EsqlBinaryComparison.class);
            assertEquals(on.getFunctionType(), EsqlBinaryComparison.BinaryComparisonOperation.EQ);
            assertEquals(as(on.left(), UnresolvedAttribute.class).name(), "f.1.f.2");
            assertEquals(as(on.right(), UnresolvedAttribute.class).name(), "f.3.f.4");
        }

        namedDoubleParams = List.of("??f1", "??f2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                "from idx1 | ENRICH idx2 ON {}.{} WITH {}.{} = {}.{}",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5)
            );
            assertEqualsIgnoringIds(
                new Enrich(
                    EMPTY,
                    relation("idx1"),
                    null,
                    Literal.keyword(EMPTY, "idx2"),
                    attribute("f.1.*.f.2"),
                    null,
                    Map.of(),
                    List.of(new Alias(EMPTY, "f.3*.f.4.*", attribute("f.5.f.6*")))
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f1", "f.1.*"),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f.3*"),
                            paramAsConstant("f4", "f.4.*"),
                            paramAsConstant("f5", "f.5"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }
    }

    public void testMixedSingleDoubleParams() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        // This is a subset of testDoubleParamsForIdentifier, with single and double parameter markers mixed in the queries
        // Single parameter markers represent a constant value or pattern
        // double parameter markers represent identifiers - field or function names

        // mixed constant and identifier, eval/where
        List<List<String>> doubleParams = new ArrayList<>(3);
        List<String> namedDoubleParams = List.of("??f0", "??fn1", "?v1", "??f2", "?v3");
        List<String> positionalDoubleParams = List.of("??1", "??2", "?3", "??4", "?5");
        List<String> anonymousDoubleParams = List.of("??", "??", "?", "??", "?");
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | eval {} = {}({})
                | where {} == {}
                | limit 1""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEqualsIgnoringIds(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(
                            EMPTY,
                            relation("test"),
                            List.of(new Alias(EMPTY, "x", function("toString", List.of(Literal.keyword(EMPTY, "constant_value")))))
                        ),
                        new Equals(EMPTY, attribute("f.2"), new Literal(EMPTY, 100, INTEGER))
                    )
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("v1", "constant_value"),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("v3", 100)
                        )
                    )
                )
            );
        }

        // mixed constant and identifier, stats/sort/mv_expand
        namedDoubleParams = List.of("??fn2", "?v3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "?2", "??3", "??4", "??5");
        anonymousDoubleParams = List.of("??", "?", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats y = {}({}) by {}
                | sort {}
                | mv_expand {}""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEqualsIgnoringIds(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.4.")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(Literal.keyword(EMPTY, "*")))), attribute("f.4."))
                        ),
                        List.of(new Order(EMPTY, attribute("f.5.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.6*"),
                    attribute("f.6*")
                ),
                query(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("v3", "*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }

        // mixed field name and field name pattern
        LogicalPlan plan = query(
            "from test | keep ??f1, ?f2 | drop ?f3, ??f4 | lookup join idx on ??f5",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "f*1."),
                    paramAsPattern("f2", "f.2*"),
                    paramAsPattern("f3", "f3.*"),
                    paramAsConstant("f4", "f.4.*"),
                    paramAsConstant("f5", "f5")
                )
            )
        );

        LookupJoin join = as(plan, LookupJoin.class);
        UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
        assertEquals(ur.indexPattern().indexPattern(), "idx");
        assertEquals(join.config().type().joinName(), "LEFT OUTER");
        assertEqualsIgnoringIds(join.config().leftFields(), List.of(attribute("f5")));
        Drop drop = as(join.left(), Drop.class);
        List<? extends NamedExpression> removals = drop.removals();
        assertEquals(removals.size(), 2);
        UnresolvedNamePattern up = as(removals.get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f3.*");
        assertEquals(up.pattern(), "f3.*");
        UnresolvedAttribute ua = as(removals.get(1), UnresolvedAttribute.class);
        assertEquals(ua.name(), "f.4.*");
        Keep keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections().size(), 2);
        ua = as(keep.projections().get(0), UnresolvedAttribute.class);
        assertEquals(ua.name(), "f*1.");
        up = as(keep.projections().get(1), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f.2*");
        assertEquals(up.pattern(), "f.2*");
        ur = as(keep.child(), UnresolvedRelation.class);
        assertEqualsIgnoringIds(ur, relation("test"));

        // test random single and double params
        // commands in group1 take both constants(?) and identifiers(??)
        List<String> commandWithRandomSingleOrDoubleParamsGroup1 = List.of(
            "eval x = {}f1, y = {}f2, z = {}f3",
            "eval x = fn({}f1), y = {}f2 + {}f3",
            "where {}f1 == \"a\" and {}f2 > 1 and {}f3 in (1, 2)",
            "stats x = fn({}f1) by {}f2, {}f3",
            "sort {}f1, {}f2, {}f3",
            "dissect {}f1 \"%{bar}\"",
            "grok {}f1 \"%{WORD:foo}\""
        );
        for (String command : commandWithRandomSingleOrDoubleParamsGroup1) {
            String param1 = randomBoolean() ? "?" : "??";
            String param2 = randomBoolean() ? "?" : "??";
            String param3 = randomBoolean() ? "?" : "??";
            plan = query(
                LoggerMessageFormat.format(null, "from test | " + command, param1, param2, param3),
                new QueryParams(List.of(paramAsConstant("f1", "f1"), paramAsConstant("f2", "f2"), paramAsConstant("f3", "f3")))
            );
            assertNotNull(plan);
        }
        // commands in group2 only take identifiers(??)
        List<String> commandWithRandomSingleOrDoubleParamsGroup2 = List.of(
            "eval x = {}f1(), y = {}f2(), z = {}f3()",
            "where {}f1 : \"b\" and {}f2() > 0 and {}f3()",
            "stats x = {}f1(), {}f2(), {}f3()",
            "rename {}f1 as {}f2, {}f3 as x",
            "enrich idx2 ON {}f1 WITH {}f2 = {}f3",
            "keep {}f1, {}f2, {}f3",
            "drop {}f1, {}f2, {}f3",
            "mv_expand {}f1 | mv_expand {}f2 | mv_expand {}f3",
            "lookup join idx1 on {}f1 | lookup join idx2 on {}f2 | lookup join idx3 on {}f3"
        );

        for (String command : commandWithRandomSingleOrDoubleParamsGroup2) {
            String param1 = randomBoolean() ? "?" : "??";
            String param2 = randomBoolean() ? "?" : "??";
            String param3 = randomBoolean() ? "?" : "??";
            if (param1.equals("?") || param2.equals("?") || param3.equals("?")) {
                if (command.contains("lookup join") == false) {
                    expectError(
                        LoggerMessageFormat.format(null, "from test | " + command, param1, param2, param3),
                        List.of(paramAsConstant("f1", "f1"), paramAsConstant("f2", "f2"), paramAsConstant("f3", "f3")),
                        "declared as a constant, cannot be used as an identifier"
                    );
                } else {
                    expectError(
                        LoggerMessageFormat.format(null, "from test | " + command, param1, param2, param3),
                        List.of(paramAsConstant("f1", "f1"), paramAsConstant("f2", "f2"), paramAsConstant("f3", "f3")),
                        "JOIN ON clause must be a comma separated list of fields or a single expression, found"
                    );

                }
            }
        }
    }

    public void testInvalidDoubleParamsNames() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        expectError(
            "from test | where x < ??n1 | eval y = ??n2",
            List.of(paramAsConstant("n1", "f1"), paramAsConstant("n3", "f2")),
            "line 1:39: Unknown query parameter [n2], did you mean any of [n3, n1]?"
        );

        expectError("from test | where x < ??@1", List.of(paramAsConstant("@1", "f1")), "line 1:25: extraneous input '@1' expecting <EOF>");

        expectError("from test | where x < ??#1", List.of(paramAsConstant("#1", "f1")), "line 1:25: token recognition error at: '#'");

        expectError("from test | where x < ??Å", List.of(paramAsConstant("Å", "f1")), "line 1:25: token recognition error at: 'Å'");

        expectError("from test | eval x = ??Å", List.of(paramAsConstant("Å", "f1")), "line 1:24: token recognition error at: 'Å'");
    }

    public void testInvalidDoubleParamsPositions() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        expectError(
            "from test | where x < ??0",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 0, did you mean position 1"
        );

        expectError(
            "from test | where x < ??2",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 2, did you mean position 1"
        );

        expectError(
            "from test | where x < ??0 and y < ??2",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 0, did you mean position 1?; "
                + "line 1:35: No parameter is defined for position 2, did you mean position 1?"
        );

        expectError(
            "from test | where x < ??0",
            List.of(paramAsConstant(null, "f1"), paramAsConstant(null, "f2")),
            "line 1:23: No parameter is defined for position 0, did you mean any position between 1 and 2?"
        );
    }

    public void testInvalidDoubleParamsType() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        // double parameter markers cannot be declared as identifier patterns
        String error = "Query parameter [??f1][f1] declared as a pattern, cannot be used as an identifier";
        List<String> commandWithDoubleParams = List.of(
            "eval x = ??f1",
            "eval x = ??f1(f1)",
            "where ??f1 == \"a\"",
            "stats x = count(??f1)",
            "sort ??f1",
            "rename ??f1 as ??f2",
            "dissect ??f1 \"%{bar}\"",
            "grok ??f1 \"%{WORD:foo}\"",
            "enrich idx2 ON ??f1 WITH ??f2 = ??f3",
            "keep ??f1",
            "drop ??f1",
            "mv_expand ??f1",
            "lookup join idx on ??f1"
        );
        for (String command : commandWithDoubleParams) {
            expectError(
                "from test | " + command,
                List.of(paramAsPattern("f1", "f1*"), paramAsPattern("f2", "f2*"), paramAsPattern("f3", "f3*")),
                error
            );
        }
    }

    public void testNullDoubleParamsValue() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        String error = "Query parameter [??f1] is null";
        List<String> commandWithDoubleParams = List.of(
            "eval x = ??f1",
            "stats x = count(??f1)",
            "sort ??f1",
            "keep ??f1",
            "drop ??f1",
            "mv_expand ??f1"
        );
        for (String command : commandWithDoubleParams) {
            expectError("from test | " + command, List.of(paramAsConstant("f1", null)), error);
        }
    }

    public void testLikeParam() {
        if (EsqlCapabilities.Cap.LIKE_PARAMETER_SUPPORT.isEnabled()) {
            LogicalPlan anonymous = query(
                // comment keeps following arguments on separate lines like other tests
                "row a = \"abc\" | where a like ?",
                new QueryParams(List.of(paramAsConstant(null, "a*")))
            );
            Filter filter = as(anonymous, Filter.class);
            WildcardLike like = as(filter.condition(), WildcardLike.class);
            assertEquals("a*", like.pattern().pattern());

            expectError(
                "row a = \"abc\" | where a like ?",
                List.of(paramAsConstant(null, 1)),
                "Invalid pattern parameter type for like [?]: expected string, found integer"
            );
            expectError(
                "row a = \"abc\" | where a like ?",
                List.of(paramAsConstant(null, List.of("a*", "b*"))),
                "Invalid pattern parameter type for like [?]: expected string, found list"
            );
        }
    }

    public void testLikeListParam() {
        if (EsqlCapabilities.Cap.LIKE_PARAMETER_SUPPORT.isEnabled()) {
            LogicalPlan positional = query(
                "row a = \"abc\" | where a like ( ?1, ?2 )",
                new QueryParams(List.of(paramAsConstant(null, "a*"), paramAsConstant(null, "b*")))
            );
            Filter filter = as(positional, Filter.class);
            WildcardLikeList likelist = as(filter.condition(), WildcardLikeList.class);
            WildcardPatternList patternlist = as(likelist.pattern(), WildcardPatternList.class);
            assertEquals("(\"a*\", \"b*\")", patternlist.pattern());

            expectError(
                "row a = \"abc\" | where a like ( ?1, ?2 )",
                List.of(paramAsConstant(null, "a*"), paramAsConstant(null, 1)),
                "Invalid pattern parameter type for like [?2]: expected string, found integer"
            );
            expectError(
                "row a = \"abc\" | where a like ( ?1, ?3 )",
                List.of(paramAsConstant(null, "a*"), paramAsConstant(null, 1)),
                "No parameter is defined for position 3, did you mean any position between 1 and 2?"
            );
        }
    }

    public void testRLikeParam() {
        if (EsqlCapabilities.Cap.LIKE_PARAMETER_SUPPORT.isEnabled()) {
            LogicalPlan named = query(
                "row a = \"abc\" | where a rlike ?pattern",
                new QueryParams(List.of(paramAsConstant("pattern", "a*")))
            );
            Filter filter = as(named, Filter.class);
            RLike rlike = as(filter.condition(), RLike.class);
            assertEquals("a*", rlike.pattern().pattern());

            expectError(
                "row a = \"abc\" | where a rlike ?pattern",
                List.of(paramAsConstant("pattern", 1)),
                "Invalid pattern parameter type for rlike [?pattern]: expected string, found integer"
            );
            expectError(
                "row a = \"abc\" | where a rlike ?pattern1",
                List.of(paramAsConstant("pattern", 1)),
                "Unknown query parameter [pattern1], did you mean [pattern]?"
            );
        }
    }

    public void testRLikeListParam() {
        if (EsqlCapabilities.Cap.LIKE_PARAMETER_SUPPORT.isEnabled()) {
            LogicalPlan named = query(
                "row a = \"abc\" | where a rlike ( ?p1, ?p2 )",
                new QueryParams(List.of(paramAsConstant("p1", "a*"), paramAsConstant("p2", "b*")))
            );
            Filter filter = as(named, Filter.class);
            RLikeList rlikelist = as(filter.condition(), RLikeList.class);
            RLikePatternList patternlist = as(rlikelist.pattern(), RLikePatternList.class);
            assertEquals("(\"a*\", \"b*\")", patternlist.pattern());

            expectError(
                "row a = \"abc\" | where a rlike ( ?p1, ?p2 )",
                List.of(paramAsConstant("p1", "a*"), paramAsConstant("p2", 1)),
                "Invalid pattern parameter type for rlike [?p2]: expected string, found integer"
            );
            expectError(
                "row a = \"abc\" | where a rlike ( ?p1, ?p3 )",
                List.of(paramAsConstant("p1", "a*"), paramAsConstant("p2", 1)),
                "Unknown query parameter [p3], did you mean any of [p1, p2]?"
            );
        }
    }
}
