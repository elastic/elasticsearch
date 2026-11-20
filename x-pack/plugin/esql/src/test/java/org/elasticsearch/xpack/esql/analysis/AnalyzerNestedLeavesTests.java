/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedLeafField;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/** A nested leaf is a column of the parent row. {@code NESTED_EXPAND} still owns that name once it runs. */
public class AnalyzerNestedLeavesTests extends AnalyzerTestCase {
    public AnalyzerNestedLeavesTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("nested leaves are snapshot-only", EsqlCapabilities.Cap.NESTED_FLATTENED_LEAVES.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testLeafIsAMultivaluedColumn() {
        LogicalPlan plan = leaves().query("FROM ne | KEEP id, items.qty, items.loc");
        assertThat(plan.output().toString(), not(containsString(NestedScopeField.NAMESPACE)));
        NestedLeafField qty = leaf(plan, "items.qty");
        assertThat(qty.path(), equalTo("items"));
        assertThat(qty.relative(), equalTo("qty"));
        assertThat(qty.isStaticallySingleValued(), equalTo(false));
        assertThat(leaf(plan, "items.loc").getDataType(), equalTo(DataType.GEO_POINT));
    }

    public void testTheNestedObjectIsNotAColumn() {
        leaves().error("FROM ne | KEEP items", containsString("Unknown column [items]"));
    }

    public void testExpandReplacesThePackedColumns() {
        LogicalPlan plan = leaves().query("FROM ne | NESTED_EXPAND items | KEEP id, items.qty");
        FieldAttribute qty = attribute(plan, "items.qty");
        assertThat(qty.field(), not(instanceOf(NestedLeafField.class)));
    }

    public void testNestedAnyStillReadsOneChild() {
        LogicalPlan plan = leaves().query("FROM ne | WHERE NESTED_ANY(items.qty > 1) | KEEP id, items.qty");
        assertThat(leaf(plan, "items.qty").relative(), equalTo("qty"));
    }

    public void testLookupIndexStaysHidden() {
        LogicalPlan plan = leaves().query("FROM ne_orders | LOOKUP JOIN ne_lookup ON id");
        EsRelation lookup = null;
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                lookup = relation;
            }
        }
        assertThat(lookup, not(equalTo(null)));
        assertThat(lookup.output().stream().anyMatch(attribute -> attribute.name().equals("items.qty")), equalTo(false));
    }

    public void testOlderNodeHidesTheLeaf() {
        nestedAnalyzer(TransportVersionUtils.randomVersionNotSupporting(NestedLeafField.VERSION)).error(
            "FROM ne | KEEP items.qty",
            containsString("Unknown column [items.qty]")
        );
    }

    private TestAnalyzer leaves() {
        return nestedAnalyzer(minimumVersionAtLeast(NestedLeafField.VERSION));
    }

    private static NestedLeafField leaf(LogicalPlan plan, String name) {
        return (NestedLeafField) attribute(plan, name).field();
    }

    private static FieldAttribute attribute(LogicalPlan plan, String name) {
        Attribute found = plan.output().stream().filter(attribute -> attribute.name().equals(name)).findFirst().orElseThrow();
        return (FieldAttribute) found;
    }
}
