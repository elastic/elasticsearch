/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;

public class QualifierTests extends AbstractStatementParserTests {
    public void testQualifiersAreDisabledInReleaseBuilds() {
        assumeFalse("Test only release builds", Build.current().isSnapshot());

        // Check that qualifiers are disabled in the qualifiedName grammar rule.field
        expectError("ROW x = 1 | WHERE [qualified].[field]", "no viable alternative at input '['");
        // Check that qualifiers are disabled in the qualifiedNamePattern grammar rule.
        expectError("ROW x = 1 | KEEP [qualified] . [field]", "no viable alternative at input '['");
        // Check that qualifiers are disabled in the LOOKUP JOIN grammar.
        expectError("ROW x = 1 | LOOKUP JOIN lu_idx AS qualified ON x", "no viable alternative at input 'lu_idx'");
        expectError("ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x", "no viable alternative at input 'lu_idx'");
    }

    /**
     * Expects
     * Filter[?[qualified].[field]]
     * \_LookupJoin[LEFT OUTER USING [?x],[],[],[],false]
     *   |_Row[[1[INTEGER] AS x#2]]
     *   \_UnresolvedRelation[lu_idx]
     * TODO: LOOKUP JOIN's qualifier will need to make it into the parsed plan node.
     */
    public void testSimpleQualifierInExpression() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String query = "ROW x = 1 | LOOKUP JOIN lu_idx AS qualified ON x | WHERE [qualified].[field]";

        LogicalPlan plan = statement(query);
        Filter filter = as(plan, Filter.class);
        LookupJoin join = as(filter.child(), LookupJoin.class);
        Row row = as(join.left(), Row.class);
        UnresolvedRelation relation = as(join.right(), UnresolvedRelation.class);

        UnresolvedAttribute filterExpr = as(filter.condition(), UnresolvedAttribute.class);
        assertEqualsIgnoringIds(new UnresolvedAttribute(Source.EMPTY, "qualified", "field", null), filterExpr);
        assertEquals("Unknown column [[qualified].[field]]", filterExpr.unresolvedMessage());

        String referenceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx AS qualified ON x | WHERE qualified.field";
        assertQualifiedAttributeInExpressions(query, "qualified", "field", 1, referenceQuery);
    }

    /**
     * Test that qualifiers are parsed correctly in various expressions.
     */
    public void testQualifiersReferencedInExpressions() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        // We do not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified ] . [ `field`]",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] > 1",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE `qualified.field` > 1"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[`field`]> 0",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field > 0"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]== [qualified].[`field`]",
            "qualified",
            "field",
            2,
            sourceQuery + "WHERE qualified.field == qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]/[qualified].[field] == 1",
            "qualified",
            "field",
            2,
            sourceQuery + "WHERE qualified.field/qualified.field == 1"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] - [qualified].[`field`] ==[qualified].[field]",
            "qualified",
            "field",
            3,
            sourceQuery + "WHERE qualified.field - qualified.field == qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE -[qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE - qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]: \"foo\"",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field: \"foo\""
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE NOT [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE NOT qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE NOT ([qualified].[field])",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE NOT qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE NOT ([qualified].[`field`])",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE NOT qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE NOT [qualified].[field] AND [qualified].[`field`]",
            "qualified",
            "field",
            2,
            sourceQuery + "WHERE NOT qualified.field AND qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] OR [qualified].[field] OR [qualified].[`field`] AND [qualified].[field]",
            "qualified",
            "field",
            4,
            sourceQuery + "WHERE qualified.field OR qualified.field OR qualified.field AND qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[`field`] IS NULL",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field IS NULL"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] IS NOT NULL",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field IS NOT NULL"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE function([qualified].[`field`]) <= other_function([qualified].[field])",
            "qualified",
            "field",
            2,
            sourceQuery + "WHERE function(qualified.field) <= other_function(qualified.field)"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]::boolean",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field::boolean"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[`field`]::boolean",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field::boolean"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] :: boolean",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field::boolean"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE ([qualified].[field])::boolean",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field::boolean"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] IN ([qualified].[field])",
            "qualified",
            "field",
            2,
            sourceQuery + "WHERE qualified.field IN (qualified.field)"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] IN ([qualified].[`field`], [qualified].[field], [qualified].[`field`])",
            "qualified",
            "field",
            4,
            sourceQuery + "WHERE qualified.field IN (qualified.field, qualified.field, qualified.field)"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field] LIKE (\"foo\", \"bar?\")",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field LIKE (\"foo\", \"bar?\")"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[`field`] RLIKE \"foo\"",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field RLIKE \"foo\""
        );
    }

    /**
     * Test that qualifiers are parsed correctly in various commands.
     */
    public void testQualifiersReferencedInCommands() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        // We do not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx AS qualified ON x | ";

        assertQualifiedAttributeInExpressions(
            sourceQuery + "CHANGE_POINT [qualified].[field] ON [qualified].[field] AS type_name, pvalue_name",
            "qualified",
            "field",
            2,
            sourceQuery + "CHANGE_POINT qualified.field ON qualified.field AS type_name, pvalue_name"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "DISSECT [qualified].[field] \"%{foo}\"",
            "qualified",
            "field",
            1,
            sourceQuery + "DISSECT qualified.field \"%{foo}\""
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "DISSECT [qualified].[`field`] \"%{foo}\"",
            "qualified",
            "field",
            1,
            sourceQuery + "DISSECT qualified.field \"%{foo}\""
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "DISSECT [qualified].[field] \"\"\"%{foo}\"\"\"",
            "qualified",
            "field",
            1,
            sourceQuery + "DISSECT qualified.field \"%{foo}\""
        );

        String keepDrop = randomBoolean() ? "KEEP" : "DROP";
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + keepDrop + " qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " [qualified].[`field`]",
            "qualified",
            "field",
            1,
            sourceQuery + keepDrop + " qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " [qualified] . [field], field, [qualified].[`field`], otherfield",
            "qualified",
            "field",
            2,
            sourceQuery + keepDrop + " qualified.field, field, qualified.field, otherfield"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " pat*ern, [qualified].[field], other_pat*ern, [qualified].[`field`], yet*other*pattern",
            "qualified",
            "field",
            2,
            sourceQuery + keepDrop + " pat*ern, qualified.field, other_pat*ern, qualified.field, yet*other*pattern"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "ENRICH policy ON [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "ENRICH policy ON qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "ENRICH policy ON [qualified].[field] WITH x = y",
            "qualified",
            "field",
            1,
            sourceQuery + "ENRICH policy ON qualified.field WITH x = y"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "EVAL qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL x = [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "EVAL x = qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL x = 2 * [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "EVAL x = 2 * (qualified.field)"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL x = [qualified].[field], y = [ qualified ] . [ field ] ",
            "qualified",
            "field",
            2,
            sourceQuery + "EVAL x = qualified.field, y = qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL x = ([qualified].[field])",
            "qualified",
            "field",
            1,
            sourceQuery + "EVAL x = qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL [qualified].[field]/[qualified].[field]",
            "qualified",
            "field",
            2,
            sourceQuery + "EVAL qualified.field/qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "EVAL x=[qualified].[field]/[qualified].[field], y = foo",
            "qualified",
            "field",
            2,
            sourceQuery + "EVAL x = qualified.field/qualified.field, y = foo"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "FORK (WHERE [qualified].[field]) (EVAL [qualified].[field]/2)",
            "qualified",
            "field",
            2,
            sourceQuery + "FORK (WHERE qualified.field) (EVAL qualified.field/2)"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "GROK [qualified].[field] \"%{WORD:foo}\"",
            "qualified",
            "field",
            1,
            sourceQuery + "GROK qualified.field \"%{WORD:foo}\""
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "GROK [qualified].[`field`] \"%{WORD:foo}\"",
            "qualified",
            "field",
            1,
            sourceQuery + "GROK qualified.field \"%{WORD:foo}\""
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "GROK [qualified] . [field] \"\"\"%{WORD:foo}\"\"\"",
            "qualified",
            "field",
            1,
            sourceQuery + "GROK qualified.field \"%{WORD:foo}\""
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "MV_EXPAND [qualified].[field]",
            "qualified",
            "field",
            2, // input and output attributes are separate
            sourceQuery + "MV_EXPAND qualified.field"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME [qualified].[field] AS foo",
            "qualified",
            "field",
            1,
            sourceQuery + "RENAME qualified.field AS foo"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME [qualified ]. [field ] AS foo, other_field AS bar",
            "qualified",
            "field",
            1,
            sourceQuery + "RENAME qualified.field AS foo, other_field AS bar"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME other_field AS bar, [ qualified ].[`field`] AS foo",
            "qualified",
            "field",
            1,
            sourceQuery + "RENAME other_field AS bar, qualified.field AS foo"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME bar = other_field, foo = [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "RENAME bar = other_field, foo = qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME [qualified].[field] AS foo, bar = [qualified].[`field`]",
            "qualified",
            "field",
            2,
            sourceQuery + "RENAME qualified.field AS foo, bar = qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME bar = [qualified].[field], [qualified].[field] AS foo",
            "qualified",
            "field",
            2,
            sourceQuery + "RENAME bar = qualified.field, qualified.field AS foo"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "RERANK score = \"query\" ON qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON [qualified].[field], [qualified].[`field`]",
            "qualified",
            "field",
            2,
            sourceQuery + "RERANK score = \"query\" ON qualified.field, qualified.`field`"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON field, [qualified ].[ field], other_field",
            "qualified",
            "field",
            1,
            sourceQuery + "RERANK score = \"query\" ON field, qualified.field, other_field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON [qualified].[field] WITH {\"inference_id\": \"foo\"}",
            "qualified",
            "field",
            1,
            sourceQuery + "RERANK score = \"query\" ON qualified.field WITH {\"inference_id\": \"foo\"}"
        );

        assertQualifiedAttributeInExpressions(
            sourceQuery + "SORT [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "SORT qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "SORT [qualified].[field]/[qualified].[field]",
            "qualified",
            "field",
            2,
            sourceQuery + "SORT qualified.field/qualified.field"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery
                + "SORT [qualified].[field] ASC, [qualified].[field] DESC, "
                + "[qualified].[field] NULLS FIRST, [qualified ]. [`field` ] NULLS LAST",
            "qualified",
            "field",
            4,
            sourceQuery + "SORT qualified.field ASC, qualified.field DESC, qualified.field NULLS FIRST, qualified.field NULLS LAST"
        );

        // The unresolved attribute in the BY gets also used as an aggregate, so we must count it twice.
        assertQualifiedAttributeInExpressions(
            sourceQuery + "STATS avg([qualified].[field]), max(1/[qualified].[field]) by [qualified].[field], 2*[qualified ].[`field` ]",
            "qualified",
            "field",
            5,
            sourceQuery + "STATS avg(qualified.field), max(1/qualified.field) by qualified.field, 2*qualified.`field`"
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery
                + "STATS avg(x) WHERE [qualified].[field], "
                + "count(y) WHERE [ qualified] . [field] != [qualified].[field] BY [qualified].[field]",
            "qualified",
            "field",
            5,
            sourceQuery + "STATS avg(x) WHERE qualified.field, count(y) WHERE qualified.field != qualified.field BY qualified.field"
        );
        // This one's a bit nonsensical because there's no aggregate function, but we still need to parse the qualified attribute as
        // UnresolvedAttribute.
        assertQualifiedAttributeInExpressions(
            sourceQuery + "STATS [qualified].[field] by [qualified].[other_field]",
            "qualified",
            "field",
            1,
            sourceQuery + "STATS qualified.field BY qualified.other_field"
        );

        // WHERE is tested extensively in testQualifiersReferencedInExpressions, so we don't need to repeat it here.
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE [qualified].[field]",
            "qualified",
            "field",
            1,
            sourceQuery + "WHERE qualified.field"
        );
    }

    public void testUnsupportedQualifiers() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        expectError("ROW [qualified].[field] = 1", "Qualified names are not supported in field definitions, found [[qualified].[field]]");

        expectError(
            sourceQuery + "CHANGE_POINT value_field ON key_field AS [qualified].[type_name], pvalue_name",
            "Qualified names are not supported in field definitions, found [[qualified].[type_name]]"
        );
        expectError(
            sourceQuery + "CHANGE_POINT value_field ON key_field AS type_name, [qualified].[pvalue_name]",
            "Qualified names are not supported in field definitions, found [[qualified].[pvalue_name]]"
        );

        expectError(
            sourceQuery + "COMPLETION [qualified].[field] = \"prompt\" WITH {\"inference_id\" : \"foo\"}",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );

        String keepDrop = randomBoolean() ? "KEEP" : "DROP";
        expectError(
            sourceQuery + keepDrop + " [qualified].[pat*ern]",
            "Qualified names are not supported in patterns, found [[qualified].[pat*ern]]"
        );
        expectError(
            sourceQuery + keepDrop + " [qual*fied].[field]",
            "Qualified names are not supported in patterns, found [[qual*fied].[field]]"
        );
        expectError(sourceQuery + keepDrop + " [qualified].[*]", "Qualified names are not supported in patterns, found [[qualified].[*]]");
        expectError(sourceQuery + keepDrop + " [qual*fied].[*]", "Qualified names are not supported in patterns, found [[qual*fied].[*]]");
        expectError(
            sourceQuery + keepDrop + " [quali*ied].[pat*ern]",
            "Qualified names are not supported in patterns, found [[quali*ied].[pat*ern]]"
        );

        expectError(
            sourceQuery + "EVAL [qualified].[field] = \"foo\"",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "EVAL field = x, [qualified].[ field] = \"foo\"",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );
        expectError(sourceQuery + "EVAL field = x, [quali*ied].[field] = \"foo\"", "no viable alternative at input '[quali*'");

        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON [qualified].[field]",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON another_field, [qualified].[field]",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON [qualified ].[ field], another_field",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx qualified ON [qualified].[field]",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx another_qualifier ON [qualified].[field]",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx AS another_qualifier ON [qualified].[field]",
            "JOIN ON clause only supports unqualified fields, found [[qualified].[field]]"
        );

        expectError(
            sourceQuery + "RENAME foo AS [qualified].[field]",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "RENAME [qualified ].[ field ] = foo",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );

        expectError(
            sourceQuery + "RERANK [qualified].[field] = \"query\" ON foo",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );

        expectError(
            sourceQuery + "STATS [qualified].[field] = avg(x)",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "STATS avg(x) by [qualified].[field] = categorize(y)",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );

        expectError(
            sourceQuery + "EVAL [qualified].[field] = 1, [ ].[qualified.field] = 2",
            "Qualified names are not supported in field definitions, found [[qualified].[field]]"
        );
    }

    public void testIllegalQualifiers() {
        assumeTrue(
            "Error messages are different when qualifiers are disabled in the grammar",
            EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled()
        );

        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        expectError(sourceQuery + "EVAL x = [`qualified`].[field]", "no viable alternative at input '[`qualified`'");
        expectError(sourceQuery + "EVAL x = [qual.ified].[field]", "no viable alternative at input '[qual.'");
        expectError(sourceQuery + "EVAL x = [qualified][field]", "no viable alternative at input '[qualified]['");
        expectError(sourceQuery + "EVAL x = [qualified.field]", "no viable alternative at input '[qualified.'");
        expectError(sourceQuery + "EVAL x = [qualified]..[field]", "no viable alternative at input '[qualified]..'");
        expectError(sourceQuery + "EVAL x = [field]", "no viable alternative at input '[field]'");
        expectError(sourceQuery + "EVAL x = qualified.[field]", "no viable alternative at input 'qualified.['");
        expectError(sourceQuery + "EVAL x = [qualified].field", "no viable alternative at input '[qualified].field'");

        expectError(sourceQuery + "EVAL [qualified].[function](x)", "mismatched input '('");

        expectError(sourceQuery + "EVAL y = [qualified].[\"foo\"]", "no viable alternative at input '[qualified].[\"foo\"'");
        expectError(sourceQuery + "EVAL y = [qualified].[TRUE]", "no viable alternative at input '[qualified].[TRUE'");
        expectError(sourceQuery + "EVAL y = [qualified].[1]", "no viable alternative at input '[qualified].[1'");
        expectError(sourceQuery + "EVAL y = [qualified].[1.2]", "no viable alternative at input '[qualified].[1.2'");
        expectError(sourceQuery + "EVAL y = [\"foo\"].[field]", "mismatched input '.'");
        expectError(sourceQuery + "EVAL y = [FALSE].[field]", "mismatched input '.'");
        expectError(sourceQuery + "EVAL y = [1].[field]", "mismatched input '.'");
        expectError(sourceQuery + "EVAL y = [1.2].[field]", "mismatched input '.'");

        expectError(
            sourceQuery + "ENRICH policy ON field WITH [qualified].[ field] = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH new_field = [qualified ].[field]",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[qualified].[field]]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH [qualified].[f*eld] = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[qualified].[f*eld]]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH [qualified].[*] = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[qualified].[*]]"
        );
        expectError(
            sourceQuery + "ENRICH policy WITH [quali*ied].[field] = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[quali*ied].[field]]"
        );
        expectError(
            sourceQuery + "ENRICH policy WITH [*].[field] = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [[*].[field]]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON [quali*ied].[field]",
            "Qualified names are not supported in patterns, found [[quali*ied].[field]]"
        );
        expectError(sourceQuery + "ENRICH policy ON [*].[field]", "Qualified names are not supported in patterns, found [[*].[field]]");

        expectError(sourceQuery + "KEEP [`qualified`].[field]", "Quoted identifiers are not supported as qualifiers, found [`qualified`]");
        expectError(sourceQuery + "KEEP [qual.ified].[field]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qu*al.ified].[field]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qual.if*ied].[field]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qual.if*ied].[fi*eld]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qualified][field]", "missing '.' at '['");
        expectError(sourceQuery + "KEEP [qual*ified][field]", "missing '.' at '['");
        expectError(sourceQuery + "KEEP [qualified][fi*eld]", "missing '.' at '['");
        expectError(sourceQuery + "KEEP [qualified.field]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qual*ified.field]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qualified.fi*eld]", "missing ']' at '.'");
        expectError(sourceQuery + "KEEP [qualified]..[field]", "extraneous input '.' expecting '['");
        expectError(sourceQuery + "KEEP [qualified].field", "missing '[' at 'field'");
        expectError(sourceQuery + "KEEP [qualified].*", "missing '[' at '*'");
        expectError(sourceQuery + "KEEP [qual*ified].field", "missing '[' at 'field'");
        expectError(sourceQuery + "KEEP qualified.[field]", "extraneous input '['");
        expectError(sourceQuery + "KEEP qual*ified.[field]", "extraneous input '['");
        expectError(sourceQuery + "KEEP qualified.[fi*eld]", "extraneous input '['");
        expectError(sourceQuery + "KEEP [field]", "mismatched input '<EOF>' expecting '.'");
        expectError(sourceQuery + "KEEP [fie*ld]", "mismatched input '<EOF>' expecting '.'");

        expectError(sourceQuery + "LIMIT [qualified].[1.2]", "no viable alternative at input '[qualified'");
        expectError(sourceQuery + "LIMIT [qualified].[field]", "no viable alternative at input '[qualified'");
        expectError(sourceQuery + "LIMIT [1.2].[field]", "mismatched input '.'");

        expectError(sourceQuery + "SAMPLE [qualified].[field]", "no viable alternative at input '[qualified'");

        // first/last are special keywords that are exceptionally allowed only in function names
        expectError(sourceQuery + "STATS [first].[field] = avg(x)", "no viable alternative at input '[first'");
        expectError(sourceQuery + "STATS [last].[field] = avg(x)", "no viable alternative at input '[last'");
        expectError(sourceQuery + "STATS [qualified].[first] = avg(x)", "no viable alternative at input '[qualified].[first'");
        expectError(sourceQuery + "STATS [qualified].[last] = avg(x)", "no viable alternative at input '[qualified].[last'");
        expectError(sourceQuery + "STATS [qualified].[first](x)", "no viable alternative at input '[qualified].[first'");
        expectError(sourceQuery + "STATS [qualified].[last](x)", "no viable alternative at input '[qualified].[last'");
        expectError(sourceQuery + "STATS [qualified].[first(x)]", "no viable alternative at input '[qualified].[first'");
        expectError(sourceQuery + "STATS [qualified].[last(x)]", "no viable alternative at input '[qualified].[last'");
    }

    /**
     * Test that unqualified attributes can also be denoted as {@code [].[fieldName]}
     */
    public void testEmptyQualifierInQualifiedName() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String sourceQuery = "ROW field = 1 | ";

        assertStatementsEqual(sourceQuery + "EVAL [].[field] = 1", sourceQuery + "EVAL field = 1");

        assertStatementsEqual(sourceQuery + "WHERE [].[field]", sourceQuery + "WHERE field");
        assertStatementsEqual(sourceQuery + "WHERE [].[field] > 1", sourceQuery + "WHERE field > 1");
        assertStatementsEqual(sourceQuery + "WHERE [ ]. [ `field`]> 0", sourceQuery + "WHERE field > 0");
        assertStatementsEqual(sourceQuery + "WHERE [ ].[field]== [].[`field`]", sourceQuery + "WHERE field == field");
        assertStatementsEqual(sourceQuery + "WHERE [].[field]/[].[field] == 1", sourceQuery + "WHERE field/field == 1");
        assertStatementsEqual(sourceQuery + "WHERE [ ].[field] - [].[`field`] ==[].[field]", sourceQuery + "WHERE field - field == field");
        assertStatementsEqual(sourceQuery + "WHERE -[ ].[field]", sourceQuery + "WHERE - field");
        assertStatementsEqual(sourceQuery + "WHERE [ ].[field]: \"foo\"", sourceQuery + "WHERE field: \"foo\"");
        assertStatementsEqual(sourceQuery + "WHERE NOT [ ].[field]", sourceQuery + "WHERE NOT field");
        assertStatementsEqual(sourceQuery + "WHERE NOT ([].[ `field` ])", sourceQuery + "WHERE NOT field");
        assertStatementsEqual(sourceQuery + "WHERE NOT [ ].[field] AND [].[`field`]", sourceQuery + "WHERE NOT field AND field");
        assertStatementsEqual(
            sourceQuery + "WHERE [ ].[field] OR [].[field] OR [].[`field`] AND [ ].[field]",
            sourceQuery + "WHERE field OR field OR field AND field"
        );
        assertStatementsEqual(sourceQuery + "WHERE [ ].[`field`] IS NULL", sourceQuery + "WHERE field IS NULL");
        assertStatementsEqual(sourceQuery + "WHERE [].[field] IS NOT NULL", sourceQuery + "WHERE field IS NOT NULL");
        assertStatementsEqual(
            sourceQuery + "WHERE function([ ].[`field`]) <= other_function([].[field])",
            sourceQuery + "WHERE function(field) <= other_function(field)"
        );
        assertStatementsEqual(sourceQuery + "WHERE [].[field]::boolean", sourceQuery + "WHERE field::boolean");
        assertStatementsEqual(sourceQuery + "WHERE [ ].[`field`]::boolean", sourceQuery + "WHERE field::boolean");
        assertStatementsEqual(sourceQuery + "WHERE ([ ].[field])::boolean", sourceQuery + "WHERE field::boolean");
        assertStatementsEqual(sourceQuery + "WHERE [ ].[field] IN ([].[field])", sourceQuery + "WHERE field IN (field)");
        assertStatementsEqual(
            sourceQuery + "WHERE [ ].[field] IN ([ ].[`field`], [].[field], [ ].[ `field` ])",
            sourceQuery + "WHERE field IN (field, field, field)"
        );
        assertStatementsEqual(
            sourceQuery + "WHERE [ ].[field] LIKE (\"foo\", \"bar?\")",
            sourceQuery + "WHERE field LIKE (\"foo\", \"bar?\")"
        );
        assertStatementsEqual(sourceQuery + "WHERE [].[`field`] RLIKE \"foo\"", sourceQuery + "WHERE field RLIKE \"foo\"");
    }

    /**
     * Test that unqualified name patterns can also be denoted as {@code [].[patter*]}
     */
    public void testEmptyQualifierInQualifiedNamePattern() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String sourceQuery = "ROW field = 1 | ";
        String keepDrop = (randomBoolean() ? "KEEP" : "DROP");

        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[field]", sourceQuery + keepDrop + " field");
        assertStatementsEqual(sourceQuery + keepDrop + " [].[ `field` ]", sourceQuery + keepDrop + " field");
        assertStatementsEqual(sourceQuery + keepDrop + " [].[ `fi``eld` ]", sourceQuery + keepDrop + " `fi``eld`");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[pat*ern]", sourceQuery + keepDrop + " pat*ern");
        assertStatementsEqual(sourceQuery + keepDrop + " [].[pat*ern]", sourceQuery + keepDrop + " pat*ern");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[ field* ]", sourceQuery + keepDrop + " field*");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[fie*ld]", sourceQuery + keepDrop + " fie*ld");
        // DROP * is disallowed, but KEEP * is allowed
        assertStatementsEqual(sourceQuery + "KEEP [ ].[*]", sourceQuery + "KEEP *");
        assertStatementsEqual(sourceQuery + "KEEP [ ].[ * ]", sourceQuery + "KEEP *");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[pat.*`ern*` ]", sourceQuery + keepDrop + " pat.*`ern*`");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[fie`l``d`]", sourceQuery + keepDrop + " fiel````d");

        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[field], field2", sourceQuery + keepDrop + " field, field2");
        assertStatementsEqual(sourceQuery + keepDrop + " [ ].[pattern1*], pattern2*", sourceQuery + keepDrop + " pattern1*, pattern2*");
        assertStatementsEqual(
            sourceQuery + keepDrop + " pat*ern1, [ ].[pattern2*], other_pat*ern",
            sourceQuery + keepDrop + " pat*ern1, pattern2*, other_pat*ern"
        );
    }

    /**
     * Assert that there is as many {@link UnresolvedAttribute}s with the given fully qualified name as expected. Then, turn all the
     * matching fully qualified {@link UnresolvedAttribute}s into unqualified attributes by removing the qualifier and prefixing the plain
     * name with it to check if we end up with the same plan as from the reference query.
     * <p>
     * Example: {@code ... | WHERE [qualified].[name] > 10} should yield the same plan as {@code ... | WHERE qualified.name > 10} after
     * flattening the qualifiers into the plain name.
     */
    private void assertQualifiedAttributeInExpressions(
        String query,
        String qualifier,
        String name,
        int expectedCount,
        String referenceQuery
    ) {
        // A regex that matches any literal brackets [, ], and spaces.
        String regex = "[\\[\\] ]*";

        assertQualifiedAttributeInExpressions(query, qualifier, name, expectedCount, referenceQuery, (expr) -> {
            if (expr instanceof UnresolvedAttribute ua) {
                String newName = ua.qualifiedName().replaceAll(regex, "");
                return new UnresolvedAttribute(ua.source(), null, newName, null);
            }
            // RERANK internally has aliases that derive their name from the score field; we need to update those, too.
            if (expr instanceof Alias alias) {
                String newName = alias.name().replaceAll(regex, "");
                return new Alias(alias.source(), newName, alias.child());
            }
            return expr;
        });
    }

    private void assertQualifiedAttributeInExpressions(
        String query,
        String qualifier,
        String name,
        int expectedCount,
        String referenceQuery,
        Function<Expression, Expression> normalizeExpressions
    ) {
        LogicalPlan plan = statement(query);
        Holder<Integer> count = new Holder<>(0);

        plan.forEachExpressionDown(UnresolvedAttribute.class, expr -> {
            int numOccurrences = countAttribute(expr, qualifier, name);
            if (numOccurrences > 0) {
                count.set(count.get() + numOccurrences);
            }
        });

        assertEquals(expectedCount, (int) count.get());

        LogicalPlan planWithStrippedQualifiers = plan.transformExpressionsDown(Expression.class, normalizeExpressions);

        LogicalPlan referencePlan = statement(referenceQuery).transformExpressionsDown(Expression.class, normalizeExpressions);

        assertEqualsIgnoringIds(referencePlan, planWithStrippedQualifiers);
    }

    private static int countAttribute(Expression expr, String qualifier, String name) {
        Holder<Integer> numOccurrences = new Holder<>(0);

        expr.forEachDown(UnresolvedAttribute.class, a -> {
            if (a.qualifier() != null && a.qualifier().equals(qualifier) && a.name().equals(name)) {
                numOccurrences.set(numOccurrences.get() + 1);
            }
        });

        return numOccurrences.get();
    }

    private void assertStatementsEqual(String query1, String query2) {
        LogicalPlan plan1 = statement(query1);
        LogicalPlan plan2 = statement(query2);
        assertEqualsIgnoringIds(plan1, plan2);
    }
}
