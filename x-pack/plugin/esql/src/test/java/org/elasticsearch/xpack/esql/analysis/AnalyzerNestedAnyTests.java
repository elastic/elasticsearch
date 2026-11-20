/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedLeafField;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedAny;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Binding and rejection of {@code NESTED_ANY} during analysis, over the indices {@link AnalyzerNestedExpandTests} describes: nested
 * {@code items} with {@code sku}, {@code qty}, {@code loc} and nested {@code items.parts}, and nested {@code events}.
 */
public class AnalyzerNestedAnyTests extends AnalyzerTestCase {
    public AnalyzerNestedAnyTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testWhereBindsToANestedPredicate() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items.sku == \"a\")");
        NestedPredicate predicate = predicate(plan);
        assertThat(predicate.path(), equalTo("items"));
        assertThat(predicate.dataType(), equalTo(DataType.BOOLEAN));
        assertThat(anyUnlowered(plan), is(false));
    }

    public void testEvalBindsToABooleanColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL hit = NESTED_ANY(items.qty > 1) | KEEP id, hit");
        assertThat(names(plan.output()), contains("id", "hit"));
        assertThat(types(plan.output()), contains(DataType.KEYWORD, DataType.BOOLEAN));
        assertThat(predicate(plan).path(), equalTo("items"));
    }

    public void testOutputHasNoNestedScope() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items.sku == \"a\")");
        assertThat(names(plan.output()), hasItems("id"));
        assertThat(names(plan.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))));
        plan.forEachDown(
            EsRelation.class,
            relation -> assertThat(names(relation.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))))
        );
    }

    public void testChildNameOutsideTheCallIsThePackedColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items.sku == \"a\") | KEEP items.sku");
        assertThat(((FieldAttribute) plan.output().getFirst()).field(), instanceOf(NestedLeafField.class));
    }

    public void testCollectionFormBindsToANestedPredicate() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL hit = NESTED_ANY(items, true) | KEEP id, hit");
        assertThat(predicate(plan).path(), equalTo("items"));
    }

    public void testCollectionFormWithAPredicateOverTheCollection() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items, items.sku == \"a\")");
        assertThat(predicate(plan).path(), equalTo("items"));
    }

    public void testSeveralCallsOverDifferentCollections() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items.sku == \"a\") AND NESTED_ANY(events.start > 1)");
        List<String> paths = new ArrayList<>();
        plan.forEachExpressionDown(NestedPredicate.class, p -> paths.add(p.path()));
        assertThat(paths, contains("items", "events"));
    }

    public void testNonBooleanBodyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | EVAL x = NESTED_ANY(items.qty) | KEEP id, x",
            containsString("first argument of [NESTED_ANY(items.qty)] must be [boolean], found value [items.qty] type [long]")
        );
    }

    public void testTwoNestedPathsAreRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items.sku == \"a\" AND events.start > 1)",
            containsString("NESTED_ANY requires one nested path")
        );
    }

    public void testMoreThanTwoArgumentsAreRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items, items.sku == \"a\", true)",
            containsString("NESTED_ANY takes a predicate, or a collection and a predicate")
        );
    }

    public void testInnerNestedAnyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(NESTED_ANY(items.sku == \"a\"))",
            containsString("NESTED_ANY does not allow inner scopes or full-text functions")
        );
    }

    public void testFullTextFunctionInTheBodyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(MATCH(items.sku, \"a\"))",
            containsString("NESTED_ANY does not allow inner scopes or full-text functions")
        );
    }

    public void testAggregateInTheBodyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(SUM(items.qty) > 1)",
            containsString("NESTED_ANY takes a predicate; use NESTED for a reduction such as SUM")
        );
    }

    public void testSortIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | SORT NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY is supported in EVAL and WHERE")
        );
    }

    public void testStatsIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | STATS c = COUNT(*) BY hit = NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY is supported in EVAL and WHERE")
        );
    }

    public void testCollectionAndAFieldOfAnotherCollectionAreRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items, events.start > 1)",
            containsString("NESTED_ANY requires one nested path")
        );
    }

    public void testNotNestedCollectionIsRejected() {
        nestedAnalyzer().error("FROM ne | WHERE NESTED_ANY(id, true)", containsString("NESTED_ANY requires a nested collection first"));
    }

    public void testOneNestingLevel() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items.parts.pn == \"a\")",
            containsString("NESTED_ANY supports one nesting level")
        );
    }

    public void testParentFieldInTheBodyIsRejected() {
        nestedAnalyzer().error("FROM ne | WHERE NESTED_ANY(items.sku == id)", containsString("Field [id] is outside the nested scope"));
    }

    public void testEvalColumnInTheBodyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | EVAL k = \"a\" | WHERE NESTED_ANY(items.sku == k)",
            containsString("Field [k] is outside the nested scope")
        );
    }

    public void testRenamedColumnInTheBodyIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | RENAME id AS pid | WHERE NESTED_ANY(items.sku == pid)",
            containsString("Field [pid] is outside the nested scope")
        );
    }

    public void testParentFieldInTheCollectionFormIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items, items.sku == id)",
            containsString("Field [id] is outside the nested scope")
        );
    }

    public void testParentFieldBesideAChildFieldIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items.sku == \"a\" AND id == \"1\")",
            containsString("Field [id] is outside the nested scope")
        );
    }

    public void testParentFieldThroughAFunctionIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(CONCAT(items.sku, id) == \"a1\")",
            containsString("Field [id] is outside the nested scope")
        );
    }

    public void testParentFieldInEvalIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | EVAL h = NESTED_ANY(items.sku == id) | KEEP id, h",
            containsString("Field [id] is outside the nested scope")
        );
    }

    public void testParentFieldAloneIsRejected() {
        nestedAnalyzer().error("FROM ne | WHERE NESTED_ANY(id == \"1\")", containsString("Field [id] is outside the nested scope"));
    }

    public void testPredicateWithoutAnyFieldIsRejected() {
        nestedAnalyzer().error("FROM ne | WHERE NESTED_ANY(true)", containsString("NESTED_ANY requires a mapped nested field"));
    }

    public void testCollectionReadAsAValueIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items IS NOT NULL)",
            containsString("NESTED_ANY requires a predicate over nested fields, not [items]")
        );
    }

    public void testNameUnderTheNestedPathIsNotAChild() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items.sku == \"a\" AND items.undeclared == \"x\")",
            containsString("Unknown column [items.undeclared]")
        );
    }

    public void testParentColumnBesideTheCallIsNotInTheBody() {
        NestedPredicate predicate = predicate(
            nestedAnalyzer().query("FROM ne | WHERE id == \"1\" AND NESTED_ANY(items.sku == \"a\") | LIMIT 10")
        );
        assertThat(predicate.inputs(), hasSize(1));
    }

    public void testEvalColumnNamedLikeAChildReplacesIt() {
        nestedAnalyzer().error(
            "FROM ne | EVAL items.sku = \"z\" | WHERE NESTED_ANY(items.sku == \"z\")",
            containsString("NESTED_ANY cannot use [items.sku]: a column of that name replaces it here")
        );
    }

    public void testRenamedColumnNamedLikeAChildReplacesIt() {
        nestedAnalyzer().error(
            "FROM ne | RENAME id AS items.sku | WHERE NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY cannot use [items.sku]: a column of that name replaces it here")
        );
    }

    public void testEvalColumnNamedLikeAChildInTheEvalForm() {
        nestedAnalyzer().error(
            "FROM ne | EVAL items.qty = 1 | EVAL h = NESTED_ANY(items.qty > 0)",
            containsString("NESTED_ANY cannot use [items.qty]: a column of that name replaces it here")
        );
    }

    public void testColumnWithAnotherNameDoesNotReplaceTheChild() {
        NestedPredicate predicate = predicate(
            nestedAnalyzer().query("FROM ne | EVAL sku = \"z\" | WHERE NESTED_ANY(items.sku == \"a\") | LIMIT 10")
        );
        assertThat(predicate.inputs(), hasSize(1));
    }

    public void testSpatialChildIsReadLikeAnyOtherColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | WHERE NESTED_ANY(items.loc IS NOT NULL)");
        assertThat(predicate(plan).inputs().stream().map(Attribute::dataType).toList(), equalTo(List.of(DataType.GEO_POINT)));
    }

    public void testTypeMismatchInTheBodyReportsTheComparison() {
        nestedAnalyzer().error(
            "FROM ne | WHERE NESTED_ANY(items.sku > 5) | KEEP id",
            containsString("first argument of [items.sku > 5] is [keyword] so second argument must also be [keyword] but was [integer]")
        );
    }

    public void testAfterStatsIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | STATS c = COUNT(*) BY id | WHERE NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY reads each row's nested documents, so it cannot follow [STATS c = COUNT(*) BY id]")
        );
    }

    public void testAfterInlineStatsIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | INLINE STATS c = COUNT(*) BY id | WHERE NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY reads each row's nested documents, so it cannot follow [INLINE STATS c = COUNT(*) BY id]")
        );
    }

    public void testCollectionOfALookupIndexIsRejected() {
        nestedAnalyzer().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | WHERE NESTED_ANY(items.qty > 1)",
            containsString("NESTED_ANY cannot read [items.qty] from a LOOKUP JOIN index")
        );
    }

    public void testNamedCollectionOfALookupIndexIsRejected() {
        nestedAnalyzer().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | WHERE NESTED_ANY(items, true)",
            containsString("NESTED_ANY cannot read [items] from a LOOKUP JOIN index")
        );
    }

    public void testCollectionOfTheDrivingIndexAfterALookupJoin() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | LOOKUP JOIN ne_lookup_note ON id | WHERE NESTED_ANY(items.sku == \"a\")");
        assertThat(predicate(plan).path(), equalTo("items"));
    }

    public void testColumnNamedLikeTheCollectionIsRejected() {
        nestedAnalyzer().error(
            "FROM ne | EVAL items = 1 | WHERE NESTED_ANY(items, items.sku == \"a\")",
            containsString("NESTED_ANY cannot use [items]: a column of that name replaces it here")
        );
    }

    public void testConflictingChildIsAmbiguousLikeAnOrdinaryColumn() {
        nestedAnalyzer().error(
            "FROM ne_types | WHERE NESTED_ANY(items.v == \"a\")",
            containsString(
                "Cannot use field [items.v] due to ambiguities being mapped as [2] incompatible types: "
                    + "[integer] in [ne_types_a], [keyword] in [ne_types_b]"
            )
        );
        nestedAnalyzer().error(
            "FROM ne_types | WHERE NESTED_ANY(items.n > 1)",
            containsString(
                "Cannot use field [items.n] due to ambiguities being mapped as [2] incompatible types: "
                    + "[integer] in [ne_types_b], [short] in [ne_types_a]"
            )
        );
    }

    public void testCastOfConflictingChildReadsEachIndexsType() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne_types | WHERE NESTED_ANY(TO_LONG(items.v) == 1) | KEEP id");
        NestedPredicate predicate = predicate(plan);
        assertThat(predicate.path(), equalTo("items"));
        assertThat(predicate.relativePaths(), equalTo(List.of("v")));
        assertThat(predicate.inputs(), hasSize(1));
        assertThat(predicate.inputs().getFirst(), instanceOf(FieldAttribute.class));
        assertThat(((FieldAttribute) predicate.inputs().getFirst()).field(), instanceOf(UnionTypeEsField.class));
        assertThat(predicate.inputs().getFirst().dataType(), equalTo(DataType.LONG));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("id")));
        List<EsRelation> relations = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relations::add);
        assertThat(relations.getFirst().output().stream().map(Attribute::name).toList(), everyItem(not(containsString("converted_to"))));
    }

    public void testSameCastOfConflictingChildIsReadOnce() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne_types | WHERE NESTED_ANY(TO_LONG(items.v) > 1 AND TO_LONG(items.v) < 5)");
        assertThat(predicate(plan).inputs(), hasSize(1));
    }

    public void testCastOfConflictingChildInTheNestedCollectionForm() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne_types | WHERE NESTED_ANY(items, TO_LONG(items.v) == 1)");
        assertThat(predicate(plan).relativePaths(), equalTo(List.of("v")));
    }

    public void testUncastConflictInsideACastedBodyIsStillAmbiguous() {
        nestedAnalyzer().error(
            "FROM ne_types | WHERE NESTED_ANY(TO_LONG(items.v) == 1 AND items.n > 1)",
            containsString("Cannot use field [items.n] due to ambiguities being mapped as [2] incompatible types")
        );
    }

    public void testSmallNumericChildIsWidenedLikeAnOrdinaryColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne_types | WHERE NESTED_ANY(items.small > 1)");
        assertThat(predicate(plan).inputs().stream().map(Attribute::dataType).toList(), equalTo(List.of(DataType.INTEGER)));
    }

    public void testIndicesDisagreeingOnTheNestedPathAreRejected() {
        nestedAnalyzer().error(
            "FROM ne_conflict | WHERE NESTED_ANY(items.sub.v == \"a\")",
            containsString("NESTED_ANY cannot read [items.sub.v]")
        );
    }

    public void testOlderNodeIsRejected() {
        nestedAnalyzer(TransportVersionUtils.randomVersionNotSupporting(NestedPredicate.VERSION)).error(
            "FROM ne | WHERE NESTED_ANY(items.sku == \"a\")",
            containsString("NESTED_ANY requires all nodes to support nested predicates")
        );
    }

    public void testUndeclaredNameInsideTheBodyIsNotInScope() {
        nestedAnalyzer().statementError(
            "SET unmapped_fields=\"load\"; FROM ne | WHERE NESTED_ANY(items.undeclared == \"a\")",
            containsString("Field [items.undeclared] is outside the nested scope")
        );
    }

    public void testNullifiedUndeclaredNameInsideTheBodyIsNotInScope() {
        nestedAnalyzer().statementError(
            "SET unmapped_fields=\"nullify\"; FROM ne | WHERE NESTED_ANY(items.undeclared == \"a\")",
            containsString("Field [items.undeclared] is outside the nested scope")
        );
    }

    public void testLoadAllUndeclaredNameInsideTheBodyIsNotInScope() {
        nestedAnalyzer().statementError(
            "SET unmapped_fields=\"load_all\"; FROM ne | WHERE NESTED_ANY(items.undeclared == \"a\")",
            containsString("Field [items.undeclared] is outside the nested scope")
        );
    }

    public void testUndeclaredNameOutsideTheCallStillLoadsFromTheParent() {
        LogicalPlan plan = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL u = items.undeclared | WHERE NESTED_ANY(items.sku == \"a\") | KEEP id, u"
        );
        List<EsRelation> relations = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relations::add);
        assertThat(relations, hasSize(1));
        assertThat(names(relations.getFirst().output()), hasItem("items.undeclared"));
    }

    public void testUndeclaredNameOutsideTheCallIsStillNullified() {
        LogicalPlan plan = nestedAnalyzer().statement(
            "SET unmapped_fields=\"nullify\"; FROM ne | EVAL u = items.undeclared | WHERE NESTED_ANY(items.sku == \"a\") | KEEP id, u"
        );
        List<EsRelation> relations = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relations::add);
        assertThat(relations, hasSize(1));
        List<Attribute> nullified = relations.getFirst().output().stream().filter(a -> a.name().equals("items.undeclared")).toList();
        assertThat(nullified, hasSize(1));
        assertThat(nullified.getFirst().dataType(), equalTo(DataType.NULL));
    }

    public void testEvalOfAChildReadsThePackedColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL u = items.sku | WHERE NESTED_ANY(items.sku == \"a\") | KEEP u");
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.KEYWORD));
    }

    private TestAnalyzer nestedAnalyzer() {
        return nestedAnalyzer(minimumVersionAtLeast(NestedLeafField.VERSION));
    }

    private static TestAnalyzer nestedAnalyzer(TransportVersion minimumVersion) {
        return AnalyzerNestedExpandTests.nestedAnalyzer(minimumVersion);
    }

    private static NestedPredicate predicate(LogicalPlan plan) {
        List<NestedPredicate> found = new ArrayList<>();
        plan.forEachExpressionDown(NestedPredicate.class, found::add);
        assertThat(found, not(empty()));
        return found.getFirst();
    }

    private static boolean anyUnlowered(LogicalPlan plan) {
        List<NestedAny> found = new ArrayList<>();
        plan.forEachExpressionDown(NestedAny.class, found::add);
        return found.isEmpty() == false;
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static List<DataType> types(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::dataType).toList();
    }
}
