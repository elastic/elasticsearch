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
import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.logsdb.datageneration.fields.FieldValues.injectNulls;
import static org.elasticsearch.logsdb.datageneration.fields.FieldValues.wrappedInArray;

public class LongFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;

    public LongFieldDataGenerator(Arbitrary arbitrary) {
        this.valueGenerator = injectNulls(arbitrary).andThen(wrappedInArray(arbitrary)).apply(arbitrary::longValue);
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> b.startObject().field("type", "long").endObject();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        return b -> b.value(valueGenerator.get());
    }
}
