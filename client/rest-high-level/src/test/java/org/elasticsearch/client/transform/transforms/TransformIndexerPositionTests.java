/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformIndexerPositionTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
                TransformIndexerPositionTests::randomTransformIndexerPosition,
                TransformIndexerPositionTests::toXContent,
                TransformIndexerPosition::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.equals("indexer_position") ||
                    field.equals("bucket_position"))
                .test();
    }

    public static TransformIndexerPosition randomTransformIndexerPosition() {
        return new TransformIndexerPosition(randomPositionMap(), randomPositionMap());
    }

    public static void toXContent(TransformIndexerPosition position, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (position.getIndexerPosition() != null) {
            builder.field("indexer_position", position.getIndexerPosition());
        }
        if (position.getBucketsPosition() != null) {
            builder.field("bucket_position", position.getBucketsPosition());
        }
        builder.endObject();
    }

    private static Map<String, Object> randomPositionMap() {
        if (randomBoolean()) {
            return null;
        }
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> position = new LinkedHashMap<>();
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
