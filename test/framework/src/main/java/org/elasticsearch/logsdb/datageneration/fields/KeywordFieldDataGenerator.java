/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class KeywordFieldDataGenerator implements FieldDataGenerator {
    public KeywordFieldDataGenerator() {
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> b.startObject().field("type", "keyword").endObject();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        return b -> b.value(randomAlphaOfLengthBetween(0, 50));
    }
}
