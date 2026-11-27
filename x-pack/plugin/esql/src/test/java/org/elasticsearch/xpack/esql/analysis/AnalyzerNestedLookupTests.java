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
import org.elasticsearch.xpack.esql.core.type.NestedContainerField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A nested collection of the lookup index of a {@code LOOKUP JOIN} is a column of the joined rows, so {@code NESTED_EXPAND} and
 * {@code NESTED_ANY} read the lookup document's nested objects. The indices are the ones {@link AnalyzerNestedExpandTests}
 * describes: {@code ne_orders} has {@code id}, and the lookup index {@code ne_lookup} has {@code id} and nested {@code items}
 * with {@code qty}.
 */
public class AnalyzerNestedLookupTests extends AnalyzerTestCase {
    public AnalyzerNestedLookupTests(VersionMode versionMode) {
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

    public void testExpandReadsTheLookupDocumentsNestedObjects() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | NESTED_EXPAND items | KEEP id, items.qty"
        );
        List<NestedExpand> expands = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, expands::add);
        assertThat(expands, hasSize(1));
        assertThat(expands.getFirst().path(), equalTo("items"));
        assertThat(names(plan.output()), contains("id", "items.qty"));
    }

    public void testAnyReadsTheLookupDocumentsNestedObjects() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | WHERE NESTED_ANY(items.qty > 1) | KEEP id"
        );
        List<NestedPredicate> predicates = new ArrayList<>();
        plan.forEachExpressionDown(NestedPredicate.class, predicates::add);
        assertThat(predicates, hasSize(1));
        assertThat(predicates.getFirst().path(), equalTo("items"));
    }

    public void testExpandReadsTheLookupCollectionThroughAnAlias() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | RENAME items AS i | NESTED_EXPAND i | KEEP id, i.qty"
        );
        assertThat(names(plan.output()), contains("id", "i.qty"));
    }

    private TestAnalyzer nestedAnalyzer() {
        return AnalyzerNestedExpandTests.nestedAnalyzer(minimumVersionAtLeast(NestedContainerField.VERSION));
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
