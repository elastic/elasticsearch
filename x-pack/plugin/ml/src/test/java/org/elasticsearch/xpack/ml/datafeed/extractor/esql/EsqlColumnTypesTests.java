/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.is;

public class EsqlColumnTypesTests extends ESTestCase {

    public void testIsDateShouldAgreeWithEsqlDataTypes() {
        for (DataType type : DataType.values()) {
            assertThat(type.typeName(), EsqlColumnTypes.isDate(type.outputType()), is(type.isDate()));
        }
    }
}
