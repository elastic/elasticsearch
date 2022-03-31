/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTermQueryTestCase<QB extends BaseTermQueryBuilder<QB>> extends AbstractQueryTestCase<QB> {

    protected abstract QB createQueryBuilder(String fieldName, Object value);

    public void testIllegalArguments() throws QueryShardException {
        String term = randomAlphaOfLengthBetween(1, 30);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createQueryBuilder(null, term));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> createQueryBuilder("", term));
        assertEquals("field name is null or empty", e.getMessage());
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
        String contentString = """
            {
                "%s" : {
                    "%s" : %s
                }
            }""".formatted(testQuery.getName(), testQuery.fieldName(), value);
        alternateVersions.put(contentString, testQuery);
        return alternateVersions;
    }
}
