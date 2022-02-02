/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ContainsAssertion extends Assertion {
    public static ContainsAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String, Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        return new ContainsAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(ContainsAssertion.class);

    public ContainsAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doAssert(Object actualValue, Object expectedValue) {
        // add support for matching objects ({a:b}) against list of objects ([ {a:b, c:d} ])
        if (expectedValue instanceof Map && actualValue instanceof List) {
            logger.trace("assert that [{}] contains [{}]", actualValue, expectedValue);
            Map<String, Object> expectedMap = (Map<String, Object>) expectedValue;
            List<Object> actualList = (List<Object>) actualValue;
            List<Map<String, Object>> actualValues = actualList.stream()
                .filter(each -> each instanceof Map)
                .map((each -> (Map<String, Object>) each))
                .filter(each -> each.keySet().containsAll(expectedMap.keySet()))
                .collect(Collectors.toList());
            assertThat(
                getField()
                    + " expected to be a list with at least one object that has keys: "
                    + expectedMap.keySet()
                    + " but it was "
                    + actualList,
                actualValues,
                is(not(empty()))
            );
            assertTrue(
                getField() + " expected to be a list with at least on object that matches " + expectedMap + " but was " + actualValues,
                actualValues.stream().anyMatch(each -> each.entrySet().containsAll(expectedMap.entrySet()))
            );
        } else {
            fail("'contains' only supports checking an object against a list of objects");
        }
    }
}
