/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class DataFrameIndexerPositionTests extends AbstractSerializingTestCase<DataFrameIndexerPosition> {

    public static DataFrameIndexerPosition randomDataFrameIndexerPosition() {
        return new DataFrameIndexerPosition(randomPosition(), randomPosition());
    }

    @Override
    protected DataFrameIndexerPosition createTestInstance() {
        return randomDataFrameIndexerPosition();
    }

    @Override
    protected Reader<DataFrameIndexerPosition> instanceReader() {
        return DataFrameIndexerPosition::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected DataFrameIndexerPosition doParseInstance(XContentParser parser) throws IOException {
        return DataFrameIndexerPosition.fromXContent(parser);
    }

    private static Map<String, Object> randomPosition() {
        if (randomBoolean()) {
            return null;
        }
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> position = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            Object value;
            if (randomBoolean()) {
                value = randomLong();
            } else {
                value = randomAlphaOfLengthBetween(1, 10);
            }
            position.put(randomAlphaOfLengthBetween(3, 10), value);
        }

        return position;
    }
}
