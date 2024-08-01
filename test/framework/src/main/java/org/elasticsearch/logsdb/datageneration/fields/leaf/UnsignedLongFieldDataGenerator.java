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
import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.Supplier;

public class UnsignedLongFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;

    public UnsignedLongFieldDataGenerator(DataSource dataSource) {
        var unsignedLongs = dataSource.get(new DataSourceRequest.UnsignedLongGenerator());
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());

        this.valueGenerator = arrays.wrapper().compose(nulls.wrapper()).apply(() -> unsignedLongs.generator().get());
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> b.startObject().field("type", "unsigned_long").endObject();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        return b -> b.value(valueGenerator.get());
    }
}
