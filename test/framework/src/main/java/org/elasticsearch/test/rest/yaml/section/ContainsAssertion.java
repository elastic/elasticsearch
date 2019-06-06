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
package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

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
        Tuple<String,Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        return new ContainsAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(ContainsAssertion.class);

    public ContainsAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        // add support for matching objects ({a:b}) against list of objects ([ {a:b, c:d} ])
        if(expectedValue instanceof Map && actualValue instanceof List) {
            logger.trace("assert that [{}] contains [{}]", actualValue, expectedValue);
            Map<String, Object> expectedMap = (Map<String, Object>) expectedValue;
            List<Object> actualList = (List<Object>) actualValue;
            List<Map<String, Object>> actualValues = actualList.stream()
                .filter(each -> each instanceof Map)
                .map((each -> (Map<String, Object>) each))
                .filter(each -> each.keySet().containsAll(expectedMap.keySet()))
                .collect(Collectors.toList());
            assertThat(
                getField() + " expected to be a list with at least one object that has keys: " +
                    expectedMap.keySet() + " but it was " + actualList,
                actualValues,
                is(not(empty()))
            );
            assertTrue(
                getField() + " expected to be a list with at least on object that matches " + expectedMap +
                    " but was " + actualValues,
                actualValues.stream()
                    .anyMatch(each -> each.entrySet().containsAll(expectedMap.entrySet()))
            );
        } else {
            fail("'contains' only supports checking an object against a list of objects");
        }
    }
}
