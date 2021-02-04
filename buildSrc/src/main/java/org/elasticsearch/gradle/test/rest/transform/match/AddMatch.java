/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformByParentArray;

import java.util.Objects;

public class AddMatch implements RestTestTransformByParentArray {
    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);
    private final String matchKey;
    private final String testName;
    private final JsonNode matchValue;
    public AddMatch(String matchKey, JsonNode matchValue, String testName) {
        this.matchKey = matchKey;
        this.matchValue = matchValue;
        this.testName = Objects.requireNonNull(testName, "adding matches is only supported for named tests");
    }

    @Override
    public String getTestName() {
        return testName;
    }

    @Override
    public void transformTest(ArrayNode matchParent) {
        ObjectNode matchObject = new ObjectNode(jsonNodeFactory);
        ObjectNode matchContent = new ObjectNode(jsonNodeFactory);
        matchContent.set(matchKey, matchValue);
        matchObject.set("match", matchContent);
        matchParent.add(matchObject);
    }

    @Override
    public String getKeyOfArrayToFind() {
        //match objects are always in the array that is the direct child of the test name, i.e.
        //"my test name" : [ {"do" : ... }, { "match" : .... }, {..}, {..}, ... ]
        return testName;
    }
}
