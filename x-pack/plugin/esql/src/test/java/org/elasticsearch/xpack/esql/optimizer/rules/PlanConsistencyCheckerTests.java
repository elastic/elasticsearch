/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.List;
import java.util.Map;

/** Source fields can be read more than once, but result names and all column ids must remain unique. */
public class PlanConsistencyCheckerTests extends ESTestCase {
    public void testDistinctSourceProjections() {
        var field = field("value");
        var other = field.withId(new NameId());
        assertFalse(check(List.of(field, other)).hasFailures());
        assertTrue(check(List.of(field, other.withId(field.id()))).hasFailures());
        assertTrue(check(List.of(field, field.withName("other"))).hasFailures());
    }

    public void testSourceFieldsDoNotHideDuplicateAliasNames() {
        var field = field("value");
        var alias = new Alias(Source.EMPTY, field.name(), field).toAttribute();
        assertTrue(check(List.of(field, alias)).hasFailures());
        assertTrue(check(List.of(alias, field)).hasFailures());
        assertTrue(check(List.of(alias, alias.withId(field.id()))).hasFailures());
    }

    public void testDifferentSourceFieldsCannotShareAName() {
        assertTrue(check(List.of(field("value"), field("other").withName("value"))).hasFailures());
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
            true
        );
    }

    private static Failures check(List<Attribute> attributes) {
        var failures = new Failures();
        PlanConsistencyChecker.checkPlan(EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(attributes), failures);
        return failures;
    }
}
