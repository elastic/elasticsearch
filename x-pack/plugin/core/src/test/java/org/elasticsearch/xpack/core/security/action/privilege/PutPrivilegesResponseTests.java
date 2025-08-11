/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PutPrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final int applicationCount = randomInt(3);
        final Map<String, List<String>> map = new HashMap<>(applicationCount);
        for (int i = 0; i < applicationCount; i++) {
            map.put(randomAlphaOfLengthBetween(3, 8), Arrays.asList(generateRandomStringArray(5, 6, false, true)));
        }
        final PutPrivilegesResponse original = new PutPrivilegesResponse(map);

        final BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);
        output.flush();
        final PutPrivilegesResponse copy = new PutPrivilegesResponse(output.bytes().streamInput());
        assertThat(copy.created(), equalTo(original.created()));
        assertJsonEquals(Strings.toString(copy), Strings.toString(original));
    }

    private void assertJsonEquals(String actual, String expected) throws IOException {
        try (
            XContentParser actualParser = createParser(JsonXContent.jsonXContent, actual);
            XContentParser expectedParser = createParser(JsonXContent.jsonXContent, expected)
        ) {
            assertThat(actualParser.mapOrdered(), equalTo(expectedParser.mapOrdered()));
        }
    }

}
