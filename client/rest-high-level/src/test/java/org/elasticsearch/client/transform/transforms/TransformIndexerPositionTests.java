/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
