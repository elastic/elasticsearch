/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AbstractXContentTestCaseTests extends ESTestCase {

    public void testInsertRandomFieldsAndShuffle() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("field", 1);
        }
        builder.endObject();
        BytesReference insertRandomFieldsAndShuffle = RandomizedContext.current().runWithPrivateRandomness(1,
                () -> AbstractXContentTestCase.insertRandomFieldsAndShuffle(
                        BytesReference.bytes(builder),
                        XContentType.JSON,
                        true,
                        new String[] {},
                        null,
                        this::createParser));
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), insertRandomFieldsAndShuffle)) {
            Map<String, Object> mapOrdered = parser.mapOrdered();
            assertThat(mapOrdered.size(), equalTo(2));
            assertThat(mapOrdered.keySet().iterator().next(), not(equalTo("field")));
        }
    }
}
