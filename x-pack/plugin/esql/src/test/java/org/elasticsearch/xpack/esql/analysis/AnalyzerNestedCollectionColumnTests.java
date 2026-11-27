/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
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
 * A nested collection is a column: {@code EVAL o = items} gives it another name, {@code RENAME} moves it, and
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} read the collection through the column. The column is not a value, so analysis
 * removes it once those commands have used it. The indices are the ones {@link AnalyzerNestedExpandTests} describes.
 */
public class AnalyzerNestedCollectionColumnTests extends AnalyzerTestCase {
    private static final String COLUMN_ERROR = "] is a nested collection [";

    public AnalyzerNestedCollectionColumnTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testExpansionReadsThroughAnAlias() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | NESTED_EXPAND o");
        NestedExpand expand = onlyExpand(plan);
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("o.loc", "o.qty", "o.sku"));
        assertThat(expand.paths(), contains("loc", "qty", "sku"));
        assertThat(names(plan.output()), hasItems("id", "o.qty", "o.sku"));
        assertNoCollectionColumnLeft("collection column", plan);
    }

    public void testExpansionReadsThroughAnAliasNamedLikeItsCollection() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL items = items | NESTED_EXPAND items");
        NestedExpand expand = onlyExpand(plan);
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("items.loc", "items.qty", "items.sku"));
        assertNoCollectionColumnLeft("collection column", plan);
    }

    public void testAliasFollowsARename() {
        NestedExpand expand = onlyExpand(nestedAnalyzer().query("FROM ne | EVAL o = items | RENAME o AS p | NESTED_EXPAND p"));
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("p.loc", "p.qty", "p.sku"));
    }

    public void testRenamingTheCollectionItself() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | RENAME items AS i | NESTED_EXPAND i");
        NestedExpand expand = onlyExpand(plan);
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("i.loc", "i.qty", "i.sku"));
        assertNoCollectionColumnLeft("collection column", plan);
    }

    public void testAliasWhenUnmappedFieldsAreNullified() {
        LogicalPlan plan = nestedAnalyzer().statement("SET unmapped_fields=\"nullify\"; FROM ne | EVAL i = items | NESTED_EXPAND i");
        assertThat(onlyExpand(plan).path(), equalTo("items"));
        assertThat(names(onlyExpand(plan).expanded()), contains("i.loc", "i.qty", "i.sku"));
    }

    public void testAnyThroughTheRenamedCollection() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | RENAME items AS i | WHERE NESTED_ANY(i, i.qty > 1)");
        assertThat(onlyPredicate(plan).path(), equalTo("items"));
    }

    public void testRenamedCollectionIsNotAValue() {
        nestedAnalyzer().error("FROM ne | RENAME items AS i | WHERE i IS NOT NULL", containsString(COLUMN_ERROR + "items]"));
    }

    public void testAliasSurvivesKeepAndDrop() {
        for (String query : List.of(
            "FROM ne | EVAL o = items | DROP id | NESTED_EXPAND o",
            "FROM ne | EVAL o = items | KEEP * | NESTED_EXPAND o"
        )) {
            assertThat(query, onlyExpand(nestedAnalyzer().query(query)).path(), equalTo("items"));
        }
    }

    public void testTwoAliasesOfTwoCollections() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL a = items, b = events | KEEP id");
        assertThat(names(plan.output()), contains("id"));
        assertNoCollectionColumnLeft("collection column", plan);
    }

    public void testALaterColumnOfTheSameNameReplacesTheEarlierAlias() {
        assertThat(
            onlyExpand(nestedAnalyzer().query("FROM ne | EVAL o = items | EVAL o = events | NESTED_EXPAND o")).path(),
            equalTo("events")
        );
        nestedAnalyzer().error(
            "FROM ne | EVAL o = items | EVAL o = 1 | NESTED_EXPAND o",
            containsString("NESTED_EXPAND requires a mapped nested field, not [o]")
        );
    }

    public void testAnyReadsThroughAnAlias() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | WHERE NESTED_ANY(o, o.sku == \"a\")");
        assertThat(onlyPredicate(plan).path(), equalTo("items"));
        assertNoCollectionColumnLeft("collection column", plan);
    }

    public void testAnyReadsThroughAnAliasWithTheCollectionsOwnNames() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | WHERE NESTED_ANY(o, items.sku == \"a\")");
        assertThat(onlyPredicate(plan).path(), equalTo("items"));
    }

    public void testAnyWithOnlyTheCollectionAndTrue() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | EVAL hit = NESTED_ANY(o, true) | KEEP id, hit");
        assertThat(onlyPredicate(plan).path(), equalTo("items"));
        assertThat(names(plan.output()), contains("id", "hit"));
    }

    public void testAnyThroughARenamedAlias() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | RENAME o AS p | WHERE NESTED_ANY(p, p.qty > 1)");
        assertThat(onlyPredicate(plan).path(), equalTo("items"));
    }

    public void testAnyAliasOfAnotherCollectionIsRejectedWithFieldsOfTheFirst() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = items | WHERE NESTED_ANY(o, events.start > 1)",
            containsString("NESTED_ANY requires one nested path")
        );
    }

    public void testAnyAliasIsNotACollectionWhenAnOrdinaryColumnReplacedIt() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = items | EVAL o = 1 | WHERE NESTED_ANY(o, true)",
            containsString("NESTED_ANY requires a nested collection first")
        );
    }

    public void testCollectionColumnIsNotAColumn() {
        for (String query : List.of(
            "FROM ne | EVAL o = items",
            "FROM ne | EVAL o = items | KEEP *",
            "FROM ne | EVAL o = items | RENAME o AS p",
            "FROM ne | EVAL o = items | NESTED_EXPAND o",
            "FROM ne | EVAL items = items"
        )) {
            LogicalPlan plan = nestedAnalyzer().query(query);
            assertThat(query, names(plan.output()), hasItem("id"));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("o"))));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("p"))));
            assertThat(query, names(plan.output()), everyItem(not(equalTo("items"))));
            assertNoCollectionColumnLeft(query, plan);
        }
    }

    public void testCollectionColumnIsNotAValue() {
        for (String query : List.of(
            "FROM ne | EVAL o = items | WHERE o IS NOT NULL",
            "FROM ne | EVAL o = items | STATS c = COUNT(*) BY o",
            "FROM ne | EVAL o = items | SORT o",
            "FROM ne | EVAL o = items | EVAL x = o | WHERE x IS NOT NULL",
            "FROM ne | EVAL o = items | KEEP id, o | EVAL x = TO_STRING(o)",
            "FROM ne | EVAL o = items | RENAME o AS p | WHERE p IS NULL",
            "FROM ne | EVAL items = items | WHERE items IS NOT NULL"
        )) {
            nestedAnalyzer().error(query, containsString(COLUMN_ERROR + "items]"));
        }
    }

    public void testBareCollectionIsNotAValueWithoutAnAlias() {
        nestedAnalyzer().error("FROM ne | KEEP items", equalTo("1:16: Unknown column [items]"));
        nestedAnalyzer().error("FROM ne | WHERE items IS NOT NULL", equalTo("1:17: Unknown column [items]"));
    }

    public void testAliasOfAMappedChildIsAnUnknownColumn() {
        nestedAnalyzer().error("FROM ne | EVAL o = items.qty", equalTo("1:20: Unknown column [items.qty]"));
    }

    public void testAliasOfAnOrdinaryFieldIsAnOrdinaryColumn() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = id | KEEP id, o");
        assertThat(names(plan.output()), contains("id", "o"));
    }

    public void testChildrenThroughTheAliasAreUnknownWithoutExpansion() {
        nestedAnalyzer().error("FROM ne | EVAL o = items | KEEP o.qty", containsString("Unknown column [o.qty]"));
        nestedAnalyzer().error("FROM ne | EVAL o = items | EVAL u = o.qty", containsString("Unknown column [o.qty]"));
    }

    public void testNameOfAnAliasCannotBeAnotherNestedCollection() {
        nestedAnalyzer().error("FROM ne | EVAL items = events", containsString("[items] cannot name the nested collection [events]"));
        nestedAnalyzer().error("FROM ne | EVAL items.qty = items", containsString("[items.qty] cannot name the nested collection [items]"));
    }

    public void testAfterStatsTheCollectionIsGone() {
        nestedAnalyzer().error("FROM ne | STATS c = COUNT(*) | EVAL o = items", containsString("Unknown column [items]"));
    }

    public void testAfterInlineStats() {
        nestedAnalyzer().error(
            "FROM ne | INLINE STATS c = COUNT(*) BY id | EVAL o = items | NESTED_EXPAND o",
            containsString("reads each row's nested documents, so it cannot follow [INLINE STATS c = COUNT(*) BY id]")
        );
    }

    public void testCollectionOfALookupIndex() {
        nestedAnalyzer().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | EVAL o = items | NESTED_EXPAND o",
            containsString("from a LOOKUP JOIN index; that index's documents are not the rows being read")
        );
    }

    public void testIndicesDisagreeOnTheNestedPath() {
        nestedAnalyzer().error("FROM ne_conflict | EVAL o = items.sub | NESTED_EXPAND o", containsString("items.sub"));
    }

    public void testNestedOneLevelOnly() {
        nestedAnalyzer().error(
            "FROM ne | EVAL o = items.parts | NESTED_EXPAND o",
            containsString("NESTED_EXPAND supports one nesting level")
        );
    }

    public void testAliasOfAnAlias() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | EVAL o = items | EVAL p = o | NESTED_EXPAND p | KEEP id, p.sku");
        assertThat(onlyExpand(plan).path(), equalTo("items"));
    }

    public void testRenameOfACollectionWhenUnmappedFieldsAreNullifiedOrLoaded() {
        for (String mode : List.of("nullify", "load")) {
            LogicalPlan plan = nestedAnalyzer().statement(
                "SET unmapped_fields=\"" + mode + "\"; FROM ne | RENAME items AS i | NESTED_EXPAND i | KEEP id, i.sku"
            );
            assertThat(mode, onlyExpand(plan).path(), equalTo("items"));
        }
    }

    public void testAliasIsNotAColumnOfAFork() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | EVAL o = items | FORK (WHERE id == \"a\") (WHERE id == \"b\") | KEEP id, _fork"
        );
        assertThat(names(plan.output()), everyItem(not(equalTo("o"))));
    }

    public void testUndeclaredNameStaysAParentFieldBesideAnAlias() {
        for (String query : List.of(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL u = items.undeclared | EVAL o = items | NESTED_EXPAND o | KEEP id, u",
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = items | NESTED_EXPAND o | EVAL u = items.undeclared | KEEP id, u"
        )) {
            LogicalPlan plan = nestedAnalyzer().statement(query);
            List<EsRelation> relations = new ArrayList<>();
            plan.forEachDown(EsRelation.class, relations::add);
            assertThat(query, names(relations.getFirst().output()), hasItem("items.undeclared"));
            assertThat(query, names(plan.output()), contains("id", "u"));
        }
    }

    public void testUndeclaredNameThroughTheAliasIsNotInTheNestedScope() {
        nestedAnalyzer().statementError(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = items | WHERE NESTED_ANY(o, o.undeclared == \"x\")",
            containsString("is outside the nested scope")
        );
    }

    public void testNameThroughTheAliasKeepsItsOrdinaryMeaningWhenLoading() {
        LogicalPlan withAlias = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = items | EVAL u = o.undeclared | KEEP id, u"
        );
        LogicalPlan withOrdinaryColumn = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL o = 1 | EVAL u = o.undeclared | KEEP id, u"
        );
        assertThat(names(withAlias.output()), equalTo(names(withOrdinaryColumn.output())));
        List<EsRelation> relations = new ArrayList<>();
        withAlias.forEachDown(EsRelation.class, relations::add);
        assertThat(names(relations.getFirst().output()), hasItem("o.undeclared"));
    }

    private TestAnalyzer nestedAnalyzer() {
        return AnalyzerNestedExpandTests.nestedAnalyzer(minimumVersionAtLeast(NestedPredicate.VERSION));
    }

    private static void assertNoCollectionColumnLeft(String query, LogicalPlan plan) {
        assertThat(names(plan.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))));
        plan.forEachDown(
            EsRelation.class,
            relation -> assertThat(names(relation.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))))
        );
        assertThat(query, plan.output().stream().filter(a -> a.dataType() == DataType.SOURCE).toList(), is(empty()));
    }

    private static NestedExpand onlyExpand(LogicalPlan plan) {
        List<NestedExpand> found = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, found::add);
        assertThat(found, hasSize(1));
        return found.getFirst();
    }

    private static NestedPredicate onlyPredicate(LogicalPlan plan) {
        List<NestedPredicate> found = new ArrayList<>();
        plan.forEachExpressionDown(NestedPredicate.class, found::add);
        assertThat(found, hasSize(1));
        return found.getFirst();
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
