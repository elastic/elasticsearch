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

package org.elasticsearch.index.query;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTermQueryTestCase<QB extends BaseTermQueryBuilder<QB>> extends AbstractQueryTestCase<QB> {

    @Override
    protected final QB doCreateTestQueryBuilder() {
        String fieldName = null;
        Object value;
        switch (randomIntBetween(0, 3)) {
            case 0:
                if (randomBoolean()) {
                    fieldName = BOOLEAN_FIELD_NAME;
                }
                value = randomBoolean();
                break;
            case 1:
                if (randomBoolean()) {
                    fieldName = STRING_FIELD_NAME;
                }
                if (frequently()) {
                    value = randomAsciiOfLengthBetween(1, 10);
                } else {
                    // generate unicode string in 10% of cases
                    JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                    value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                }
                break;
            case 2:
                if (randomBoolean()) {
                    fieldName = INT_FIELD_NAME;
                }
                value = randomInt(10000);
                break;
            case 3:
                if (randomBoolean()) {
                    fieldName = DOUBLE_FIELD_NAME;
                }
                value = randomDouble();
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (fieldName == null) {
            fieldName = randomAsciiOfLengthBetween(1, 10);
        }
        return createQueryBuilder(fieldName, value);
    }

    protected abstract QB createQueryBuilder(String fieldName, Object value);

    @Test
    public void testIllegalArguments() throws QueryShardException {
        try {
            if (randomBoolean()) {
                createQueryBuilder(null, randomAsciiOfLengthBetween(1, 30));
            } else {
                createQueryBuilder("", randomAsciiOfLengthBetween(1, 30));
            }
            fail("fieldname cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            createQueryBuilder("field", null);
            fail("value cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Override
    protected Map<String, QB> getAlternateVersions() {
        HashMap<String, QB> alternateVersions = new HashMap<>();
        QB tempQuery = createTestQueryBuilder();
        QB testQuery = createQueryBuilder(tempQuery.fieldName(), tempQuery.value());
        boolean isString = testQuery.value() instanceof String;
        Object value;
        if (isString) {
            JsonStringEncoder encoder = JsonStringEncoder.getInstance();
            value = "\"" + new String(encoder.quoteAsString((String) testQuery.value())) + "\"";
        } else {
            value = testQuery.value();
        }
        String contentString = "{\n" +
                "    \"" + testQuery.getName() + "\" : {\n" +
                "        \"" + testQuery.fieldName() + "\" : " + value + "\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, testQuery);
        return alternateVersions;
    }
}
