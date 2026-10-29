/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomDateEsField;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.hamcrest.Matchers.is;

public class DateEsFieldTests extends AbstractEsFieldTypeTests<DateEsField> {
    @Override
    protected DateEsField createTestInstance() {
        return randomDateEsField(4);
    }

    public void testSingleValuedSerialization() throws Exception {
        var field = DateEsField.dateEsField("d", Map.of(), true, EsField.TimeSeriesFieldType.NONE, true);
        assertThat(copyInstance(field, EsField.ESQL_SERIALIZE_SINGLE_VALUED_FIELD).isStaticallySingleValued(), is(true));
        assertThat(
            copyInstance(field, TransportVersionUtils.getPreviousVersion(EsField.ESQL_SERIALIZE_SINGLE_VALUED_FIELD))
                .isStaticallySingleValued(),
            is(false)
        );
    }

    @Override
    protected DateEsField mutateInstance(DateEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        boolean isStaticallySingleValued = instance.isStaticallySingleValued();
        switch (between(0, 4)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> aggregatable = false == aggregatable;
            case 3 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 4 -> isStaticallySingleValued = false == isStaticallySingleValued;
            default -> throw new IllegalArgumentException();
        }
        return DateEsField.dateEsField(name, properties, aggregatable, tsType, isStaticallySingleValued);
    }
}
