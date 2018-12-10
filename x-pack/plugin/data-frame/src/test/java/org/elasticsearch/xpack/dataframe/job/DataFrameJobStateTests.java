/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.job;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataFrameJobStateTests extends AbstractSerializingTestCase<DataFrameJobState> {

    public static DataFrameJobState randomDataFrameJobState() {
        return new DataFrameJobState(randomFrom(IndexerState.values()), randomPosition(), randomLongBetween(0,10));
    }

    @Override
    protected DataFrameJobState doParseInstance(XContentParser parser) throws IOException {
        return DataFrameJobState.fromXContent(parser);
    }

    @Override
    protected DataFrameJobState createTestInstance() {
        return randomDataFrameJobState();
    }

    @Override
    protected Reader<DataFrameJobState> instanceReader() {
        return DataFrameJobState::new;
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
