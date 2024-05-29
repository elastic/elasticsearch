/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TransformIndexerPositionTests extends AbstractXContentSerializingTestCase<TransformIndexerPosition> {

    public static TransformIndexerPosition randomTransformIndexerPosition() {
        return new TransformIndexerPosition(randomPosition(), randomPosition());
    }

    @Override
    protected TransformIndexerPosition createTestInstance() {
        return randomTransformIndexerPosition();
    }

    @Override
    protected TransformIndexerPosition mutateInstance(TransformIndexerPosition instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TransformIndexerPosition> instanceReader() {
        return TransformIndexerPosition::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TransformIndexerPosition doParseInstance(XContentParser parser) throws IOException {
        return TransformIndexerPosition.fromXContent(parser);
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
