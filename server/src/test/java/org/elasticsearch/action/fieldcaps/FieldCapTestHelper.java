/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.junit.Assert.assertNotNull;

class FieldCapTestHelper {

    public static FieldCapabilitiesIndexResponse createRandomIndexResponse() {
        return randomIndexResponse(randomAsciiLettersOfLength(10), randomBoolean());
    }

    public static FieldCapabilitiesIndexResponse randomIndexResponse(String index, boolean canMatch) {
        Map<String, IndexFieldCapabilities> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            responses.put(field, randomFieldCaps(field));
        }
        return new FieldCapabilitiesIndexResponse(index, responses, canMatch);
    }

    public static IndexFieldCapabilities randomFieldCaps(String fieldName) {
        Map<String, String> meta;
        switch (randomInt(2)) {
            case 0:
                meta = Collections.emptyMap();
                break;
            case 1:
                meta = Collections.singletonMap("key", "value");
                break;
            default:
                meta = new HashMap<>();
                meta.put("key1", "value1");
                meta.put("key2", "value2");
                break;
        }

        return new IndexFieldCapabilities(fieldName, randomAlphaOfLengthBetween(5, 20),
            randomBoolean(), randomBoolean(), randomBoolean(), meta);
    }

}
