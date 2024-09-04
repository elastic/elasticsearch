/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields.leaf;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public class HalfFloatFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;
    private final Map<String, Object> mappingParameters;

    public HalfFloatFieldDataGenerator(String fieldName, DataSource dataSource) {
        var halfFloats = dataSource.get(new DataSourceRequest.HalfFloatGenerator());
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());

        this.valueGenerator = arrays.wrapper().compose(nulls.wrapper()).apply(() -> halfFloats.generator().get());
        this.mappingParameters = dataSource.get(new DataSourceRequest.LeafMappingParametersGenerator(fieldName, FieldType.HALF_FLOAT))
            .mappingGenerator()
            .get();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> {
            b.startObject().field("type", "half_float");

            for (var entry : mappingParameters.entrySet()) {
                b.field(entry.getKey(), entry.getValue());
            }

            b.endObject();
        };
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        return b -> b.value(valueGenerator.get());
    }
}
