/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FetchOperatorFactoryTests extends ESTestCase {
    /**
     * The nodes that own the documents load the fetched columns without an extraction preference, so a point arrives as
     * its well known binary and not as the long of its doc values. The operator asks them for exactly those columns.
     */
    public void testLoadsPlainValues() {
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        List<Attribute> fetched = List.of(field("message", DataType.KEYWORD), field("location", DataType.GEO_POINT));
        FetchExec exec = new FetchExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            fetched,
            "idx",
            List.of("idx"),
            null
        );

        FetchOperator.Factory factory = FetchOperator.Factory.forExec(exec, 0, null);
        assertThat(factory.fetchedTypes(), equalTo(List.of(ElementType.BYTES_REF, ElementType.BYTES_REF)));
        assertThat(factory.attributes(), equalTo(fetched));
        assertThat(factory.describe(), equalTo("FetchOperator[docChannel=0, fetchedTypes=[BYTES_REF, BYTES_REF]]"));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
