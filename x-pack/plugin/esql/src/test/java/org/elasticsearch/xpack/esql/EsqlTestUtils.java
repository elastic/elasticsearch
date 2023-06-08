/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.hamcrest.Matchers.instanceOf;

public final class EsqlTestUtils {

    public static final EsqlConfiguration TEST_CFG = new EsqlConfiguration(
        DateUtils.UTC,
        null,
        null,
        new QueryPragmas(Settings.EMPTY),
        EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY)
    );

    private EsqlTestUtils() {}

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, emptyList(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan localSource(List<Attribute> fields, List<Object> row) {
        return new LocalRelation(Source.EMPTY, fields, LocalSupplier.of(BlockUtils.fromListRow(row)));
    }

    public static <T> T as(Object node, Class<T> type) {
        Assert.assertThat(node, instanceOf(type));
        return type.cast(node);
    }

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(EsqlDataTypeRegistry.INSTANCE, name, true);
    }

    public static EnrichResolution emptyPolicyResolution() {
        return new EnrichResolution(Set.of(), Set.of());
    }
}
