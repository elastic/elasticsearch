/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.util.HashMap;
import java.util.Map;

public class RollupJobStatusTests extends AbstractXContentSerializingTestCase<RollupJobStatus> {
    private Map<String, Object> randomPosition() {
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

    @Override
    protected RollupJobStatus createTestInstance() {
        return new RollupJobStatus(randomFrom(IndexerState.values()), randomPosition());
    }

    @Override
    protected RollupJobStatus mutateInstance(RollupJobStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RollupJobStatus> instanceReader() {
        return RollupJobStatus::new;
    }

    @Override
    protected RollupJobStatus doParseInstance(XContentParser parser) {
        return RollupJobStatus.fromXContent(parser);
    }

}
