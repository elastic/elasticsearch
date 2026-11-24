/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;

import java.util.List;

public class FunctionGeneratorUtilsTests extends ESTestCase {

    public void testSourceColumnsAreNotScalarArguments() {
        List<Column> columns = List.of(
            new Column("_source", DataType.SOURCE.outputType(), List.of()),
            new Column("message", "keyword", List.of())
        );
        for (int i = 0; i < 100; i++) {
            assertEquals("message", EsqlQueryGenerator.unquote(FunctionGeneratorUtils.randomScalarField(columns)));
        }
    }

    public void testOnlySourceColumnsLeaveNoScalarArgument() {
        assertNull(FunctionGeneratorUtils.randomScalarField(List.of(new Column("_source", DataType.SOURCE.outputType(), List.of()))));
    }
}
