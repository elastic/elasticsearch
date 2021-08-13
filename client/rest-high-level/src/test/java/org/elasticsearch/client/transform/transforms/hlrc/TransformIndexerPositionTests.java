/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransformIndexerPositionTests extends AbstractResponseTestCase<
        TransformIndexerPosition,
        org.elasticsearch.client.transform.transforms.TransformIndexerPosition> {

    public static TransformIndexerPosition fromHlrc(
            org.elasticsearch.client.transform.transforms.TransformIndexerPosition instance) {
        if (instance == null) {
            return null;
        }
        return new TransformIndexerPosition(instance.getIndexerPosition(), instance.getBucketsPosition());
    }

    public static TransformIndexerPosition randomTransformIndexerPosition() {
        return new TransformIndexerPosition(randomPositionMap(), randomPositionMap());
    }

    @Override
    protected TransformIndexerPosition createServerTestInstance(XContentType xContentType) {
        return randomTransformIndexerPosition();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.TransformIndexerPosition doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.TransformIndexerPosition.fromXContent(parser);
    }

    @Override
    protected void assertInstances(TransformIndexerPosition serverTestInstance,
                                   org.elasticsearch.client.transform.transforms.TransformIndexerPosition clientInstance) {
        assertThat(serverTestInstance.getIndexerPosition(), equalTo(clientInstance.getIndexerPosition()));
        assertThat(serverTestInstance.getBucketsPosition(), equalTo(clientInstance.getBucketsPosition()));
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
