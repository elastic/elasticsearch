/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedObject;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * {@code NESTED_OBJECT} names a nested collection so that later commands can read it through the name. Analysis resolves those
 * names and then removes the reference, so it is never a column. The indices are the ones {@link AnalyzerNestedExpandTests}
 * describes: {@code ne} has {@code id}, nested {@code items} with {@code sku}, {@code qty}, {@code loc} and nested
 * {@code items.parts}, and nested {@code events} with {@code start}.
 */
public class AnalyzerNestedObjectTests extends AnalyzerTestCase {
    private static final String REFERENCE_ERROR = "] is a reference to the nested collection [";

    public AnalyzerNestedObjectTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireSnapshot() {
        assumeTrue("NESTED_OBJECT is snapshot-only", Build.current().isSnapshot());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testReferenceNamedLikeItsCollectionIsKept() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL items = NESTED_OBJECT(items) | KEEP items");
        assertThat(names(plan.output()), is(empty()));
        assertNoReferenceLeft(plan);
    }

    public void testReferenceNamedLikeItsCollectionBesideOtherColumns() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL items = NESTED_OBJECT(items) | KEEP id, items");
        assertThat(names(plan.output()), contains("id"));
        assertNoReferenceLeft(plan);
    }

    public void testExpansionReadsThroughAReferenceNamedLikeItsCollection() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL items = NESTED_OBJECT(items) | NESTED_EXPAND items");
        NestedExpand expand = onlyExpand(plan);
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("items.loc", "items.qty", "items.sku"));
        assertThat(names(plan.output()), hasItems("id", "items.qty", "items.sku"));
        assertNoReferenceLeft(plan);
    }

    public void testExpansionNamesItsColumnsAfterTheReference() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = NESTED_OBJECT(items) | NESTED_EXPAND o");
        NestedExpand expand = onlyExpand(plan);
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("o.loc", "o.qty", "o.sku"));
        assertThat(expand.paths(), contains("loc", "qty", "sku"));
        assertThat(names(plan.output()), hasItems("id", "o.qty", "o.sku"));
        assertNoReferenceLeft(plan);
    }

    public void testReferenceFollowsARename() {
        NestedExpand expand = onlyExpand(
            nestedAnalyzer().query("FROM ne | EVAL o = NESTED_OBJECT(items) | RENAME o AS p | NESTED_EXPAND p")
        );
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("p.loc", "p.qty", "p.sku"));
    }

    public void testReferenceSurvivesKeepAndDrop() {
        for (String query : List.of(
            "FROM ne | EVAL o = NESTED_OBJECT(items) | DROP id | NESTED_EXPAND o",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | KEEP * | NESTED_EXPAND o"
        )) {
            assertThat(query, onlyExpand(nestedAnalyzer().query(query)).path(), equalTo("items"));
        }
    }

    public void testTwoReferencesToTwoCollections() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL a = NESTED_OBJECT(items), b = NESTED_OBJECT(events) | KEEP id");
        assertThat(names(plan.output()), contains("id"));
        assertNoReferenceLeft(plan);
    }

    public void testALaterReferenceOfTheSameNameReplacesTheEarlierOne() {
        NestedExpand expand = onlyExpand(
            nestedAnalyzer().query("FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL o = NESTED_OBJECT(events) | NESTED_EXPAND o")
        );
        assertThat(expand.path(), equalTo("events"));
    }

    public void testOrdinaryColumnReplacingAReferenceIsNotAReference() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL o = 1 | NESTED_EXPAND o",
            containsString("NESTED_EXPAND requires a mapped nested field, not [o]")
        );
    }

    public void testReferenceIsNotAColumn() {
        for (String query : List.of(
            "FROM ne | EVAL o = NESTED_OBJECT(items)",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | KEEP id, o",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | KEEP *",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | RENAME o AS p",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | NESTED_EXPAND o",
            "FROM ne | EVAL items = NESTED_OBJECT(items)"
        )) {
            LogicalPlan plan = nestedAnalyzer().query(query);
            assertThat(query, names(plan.output()), hasItem("id"));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("o"))));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("p"))));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("items"))));
            assertNoReferenceLeft(plan);
        }
    }

    public void testArgumentIsNotACollection() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = NESTED_OBJECT(items.qty)",
            containsString("NESTED_OBJECT requires a nested collection, not [items.qty]")
        );
        nestedAnalyzer().error(
            "FROM ne | EVAL o = NESTED_OBJECT(id)",
            containsString("NESTED_OBJECT requires a nested collection, not [id]")
        );
        nestedAnalyzer().error(
            "FROM ne | EVAL o = NESTED_OBJECT(items.qty + 1)",
            containsString("NESTED_OBJECT requires a nested collection, not [items.qty + 1]")
        );
    }

    public void testArgumentIsAnUnknownName() {
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(missing)", containsString("Unknown column [missing]"));
    }

    public void testUnknownColumnAfterAReferenceIsReported() {
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items) | KEEP missing", containsString("Unknown column [missing]"));
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL x = missing", containsString("Unknown column [missing]"));
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items) | WHERE missing > 1", containsString("Unknown column [missing]"));
    }

    public void testOnlyInEval() {
        nestedAnalyzer().error("FROM ne | WHERE NESTED_OBJECT(items) IS NOT NULL", containsString("NESTED_OBJECT is supported in EVAL"));
        nestedAnalyzer().error(
            "FROM ne | STATS c = COUNT(*) BY o = NESTED_OBJECT(items)",
            containsString("NESTED_OBJECT is supported in EVAL")
        );
        nestedAnalyzer().error("FROM ne | SORT NESTED_OBJECT(items)", containsString("NESTED_OBJECT is supported in EVAL"));
    }

    public void testOnlyAsTheWholeAssignment() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = COALESCE(NESTED_OBJECT(items), NESTED_OBJECT(events))",
            containsString("NESTED_OBJECT must be the whole value of an EVAL column")
        );
    }

    public void testReferenceIsNotAValue() {
        for (String query : List.of(
            "FROM ne | EVAL o = NESTED_OBJECT(items) | WHERE o IS NOT NULL",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | STATS c = COUNT(*) BY o",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | SORT o",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL x = o",
            "FROM ne | EVAL o = NESTED_OBJECT(items) | RENAME o AS p | WHERE p IS NULL",
            "FROM ne | EVAL items = NESTED_OBJECT(items) | WHERE items IS NOT NULL"
        )) {
            nestedAnalyzer().error(query, containsString(REFERENCE_ERROR + "items]"));
        }
    }

    public void testNameOfAReferenceCannotBeAnotherNestedCollection() {
        nestedAnalyzer().error(
            "FROM ne | EVAL items = NESTED_OBJECT(events)",
            containsString("NESTED_OBJECT cannot be named [items]: that name is a nested collection")
        );
        nestedAnalyzer().error(
            "FROM ne | EVAL items.qty = NESTED_OBJECT(items)",
            containsString("NESTED_OBJECT cannot be named [items.qty]: that name is a nested collection")
        );
    }

    public void testAfterStats() {
        nestedAnalyzer().error("FROM ne | STATS c = COUNT(*) | EVAL o = NESTED_OBJECT(items)", containsString("Unknown column [items]"));
    }

    public void testAfterInlineStats() {
        nestedAnalyzer().error(
            "FROM ne | INLINE STATS c = COUNT(*) BY id | EVAL o = NESTED_OBJECT(items)",
            containsString("NESTED_OBJECT reads each row's nested documents, so it cannot follow [INLINE STATS c = COUNT(*) BY id]")
        );
    }

    public void testCollectionOfALookupIndex() {
        nestedAnalyzer().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | EVAL o = NESTED_OBJECT(items)",
            containsString("NESTED_OBJECT cannot read [items] from a LOOKUP JOIN index; that index's documents are not the rows being read")
        );
    }

    public void testIndicesDisagreeOnTheNestedPath() {
        nestedAnalyzer().error(
            "FROM ne_conflict | EVAL o = NESTED_OBJECT(items.sub)",
            containsString("NESTED_OBJECT cannot read [items.sub]")
        );
    }

    public void testReferenceIsNotAColumnOfAFork() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | EVAL o = NESTED_OBJECT(items) | FORK (WHERE id == \"a\") (WHERE id == \"b\") | KEEP id, _fork"
        );
        assertThat(names(plan.output()), everyItem(not(equalTo("o"))));
        assertThat(plan.output().stream().filter(a -> a.dataType() == DataType.OBJECT).toList(), is(empty()));
    }

    public void testNestedOneLevelOnly() {
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items.parts)", containsString("NESTED_OBJECT supports one nesting level"));
    }

    public void testMappedChildThroughTheReferenceIsUnknown() {
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items) | KEEP o.qty", containsString("Unknown column [o.qty]"));
        nestedAnalyzer().error("FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL u = o.qty", containsString("Unknown column [o.qty]"));
    }

    public void testUndeclaredNameStaysAParentFieldBesideAReference() {
        for (String query : List.of(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL u = items.undeclared | EVAL o = NESTED_OBJECT(items)"
                + " | NESTED_EXPAND o | KEEP id, u",
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = NESTED_OBJECT(items) | NESTED_EXPAND o"
                + " | EVAL u = items.undeclared | KEEP id, u"
        )) {
            LogicalPlan plan = nestedAnalyzer().statement(query);
            List<EsRelation> relations = new ArrayList<>();
            plan.forEachDown(EsRelation.class, relations::add);
            assertThat(query, names(relations.getFirst().output()), hasItem("items.undeclared"));
            assertThat(query, names(plan.output()), contains("id", "u"));
        }
    }

    public void testUnmappedNameOutsideTheNestedCommandsKeepsItsOrdinaryMeaningWhenLoading() {
        LogicalPlan withReference = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = NESTED_OBJECT(items) | EVAL u = o.undeclared | KEEP id, u"
        );
        LogicalPlan withOrdinaryColumn = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = 1 | EVAL u = o.undeclared | KEEP id, u"
        );
        assertThat(names(withReference.output()), equalTo(names(withOrdinaryColumn.output())));
        List<EsRelation> relations = new ArrayList<>();
        withReference.forEachDown(EsRelation.class, relations::add);
        assertThat(names(relations.getFirst().output()), hasItem("o.undeclared"));
    }

    private TestAnalyzer nestedAnalyzer() {
        return AnalyzerNestedExpandTests.nestedAnalyzer(minimumVersion);
    }

    private static void assertNoReferenceLeft(LogicalPlan plan) {
        assertThat(plan.output().stream().filter(a -> a.dataType() == DataType.OBJECT).toList(), is(empty()));
        assertThat(names(plan.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))));
        List<NestedObject> unlowered = new ArrayList<>();
        plan.forEachExpressionDown(NestedObject.class, unlowered::add);
        assertThat(unlowered, is(empty()));
        plan.forEachDown(
            EsRelation.class,
            relation -> assertThat(names(relation.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))))
        );
    }

    private static NestedExpand onlyExpand(LogicalPlan plan) {
        List<NestedExpand> found = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, found::add);
        assertThat(found, hasSize(1));
        return found.getFirst();
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
