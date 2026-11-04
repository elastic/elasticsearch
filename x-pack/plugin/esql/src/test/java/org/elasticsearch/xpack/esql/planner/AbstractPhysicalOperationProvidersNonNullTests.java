/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Tests which group attributes {@link AbstractPhysicalOperationProviders#isNonNull} lets a hash treat as having no null.
 */
public class AbstractPhysicalOperationProvidersNonNullTests extends ESTestCase {

    public void testNonNullableFieldIsNonNull() {
        assertThat(AbstractPhysicalOperationProviders.isNonNull(field(true)), is(true));
    }

    public void testFieldWithoutTheGuaranteeIsNotNonNull() {
        assertThat(AbstractPhysicalOperationProviders.isNonNull(field(false)), is(false));
    }

    /** Only a field comes with the guarantee of a mapping. A reference, for example to an {@code EVAL}, does not. */
    public void testReferenceIsNotNonNull() {
        Attribute reference = new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER);
        assertThat(AbstractPhysicalOperationProvidersNonNullTests.class.getSimpleName(), reference instanceof FieldAttribute, is(false));
        assertThat(AbstractPhysicalOperationProviders.isNonNull(reference), is(false));
    }

    /** Widening a short to an integer, which ES|QL does when it reads the field, keeps the guarantee. */
    public void testWidenedSmallNumericIsNonNull() {
        EsField widened = new EsField("x", DataType.SHORT, Map.of(), true, false, EsField.TimeSeriesFieldType.NONE, false, true)
            .withWidenedSmallNumeric();
        assertThat(AbstractPhysicalOperationProviders.isNonNull(new FieldAttribute(Source.EMPTY, null, null, "x", widened)), is(true));
    }

    private static FieldAttribute field(boolean nonNullable) {
        EsField field = new EsField("x", DataType.INTEGER, Map.of(), true, false, EsField.TimeSeriesFieldType.NONE, false, nonNullable);
        return new FieldAttribute(Source.EMPTY, null, null, "x", field);
    }
}
