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
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class AllocateActionTests extends AbstractXContentTestCase<AllocateAction> {

    @Override
    protected AllocateAction createTestInstance() {
        return randomInstance();
    }

    static AllocateAction randomInstance() {
        boolean hasAtLeastOneMap = false;
        Map<String, String> includes;
        if (randomBoolean()) {
            includes = randomMap(1, 100);
            hasAtLeastOneMap = true;
        } else {
            includes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> excludes;
        if (randomBoolean()) {
            hasAtLeastOneMap = true;
            excludes = randomMap(1, 100);
        } else {
            excludes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> requires;
        if (hasAtLeastOneMap == false || randomBoolean()) {
            requires = randomMap(1, 100);
        } else {
            requires = randomBoolean() ? null : Collections.emptyMap();
        }
        Integer numberOfReplicas = randomBoolean() ? null : randomIntBetween(0, 10);
        return new AllocateAction(numberOfReplicas, includes, excludes, requires);
    }

    @Override
    protected AllocateAction doParseInstance(XContentParser parser) {
        return AllocateAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // this whole structure expects to be maps of strings, so more complex objects would just mess that up.
        // setting it this way allows for new fields at the root
        return (field) -> field.isEmpty() == false;
    }

    public void testAllMapsNullOrEmpty() {
        Map<String, String> include = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> exclude = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> require = randomBoolean() ? null : Collections.emptyMap();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new AllocateAction(null, include, exclude, require));
        assertEquals("At least one of " + AllocateAction.INCLUDE_FIELD.getPreferredName() + ", "
            + AllocateAction.EXCLUDE_FIELD.getPreferredName() + " or " + AllocateAction.REQUIRE_FIELD.getPreferredName()
            + "must contain attributes for action " + AllocateAction.NAME, exception.getMessage());
    }

    public void testInvalidNumberOfReplicas() {
        Map<String, String> include = randomMap(1, 5);
        Map<String, String> exclude = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> require = randomBoolean() ? null : Collections.emptyMap();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new AllocateAction(randomIntBetween(-1000, -1), include, exclude, require));
        assertEquals("[" + AllocateAction.NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0", exception.getMessage());
    }

    public static Map<String, String> randomMap(int minEntries, int maxEntries) {
        Map<String, String> map = new HashMap<>();
        int numIncludes = randomIntBetween(minEntries, maxEntries);
        for (int i = 0; i < numIncludes; i++) {
            map.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
        return map;
    }
}
