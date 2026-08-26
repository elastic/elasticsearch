/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PackDimsAggSerializationTests extends AbstractExpressionSerializationTests<PackDimsAgg> {
    @Override
    protected PackDimsAgg createTestInstance() {
        int n = between(1, 3);
        List<Expression> dims = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            dims.add(randomDimension());
        }
        return PackDimsAgg.create(randomSource(), dims);
    }

    @Override
    protected PackDimsAgg mutateInstance(PackDimsAgg instance) throws IOException {
        List<Expression> dims = new ArrayList<>(instance.dims());
        dims.set(0, randomValueOtherThan(dims.get(0), this::randomDimension));
        return PackDimsAgg.create(instance.source(), dims);
    }

    private Expression randomDimension() {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            randomAlphaOfLength(5),
            new EsField(randomAlphaOfLength(5), DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            randomBoolean()
        );
    }
}
