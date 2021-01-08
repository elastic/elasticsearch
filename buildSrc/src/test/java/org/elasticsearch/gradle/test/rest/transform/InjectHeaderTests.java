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

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;


public class InjectHeaderTests extends GradleUnitTestCase {

    private static final YAMLFactory yaml = new YAMLFactory();
    private static final ObjectMapper mapper = new ObjectMapper(yaml);


    @Test
    public void testInjectHeadersWithoutSetupBlock() throws Exception {

        File testFile = new File(getClass().getResource("/rest/header_inject_no_setup.yml").toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        Map<String, String> headers =
            Map.of("Content-Type", "application/vnd.elasticsearch+json;compatible-with=7",
                "Accept", "application/vnd.elasticsearch+json;compatible-with=7"
            );

        List<ObjectNode> transformedTests = transformer.transformRestTests(new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers)));
        //ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetupForHeaders);
        //ensure do body is correct
        transformedTests.stream()
            .filter(node -> node.get("setup") == null)
            .filter(node -> node.get("teardown") == null)
            .forEach(test -> {
                Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
                while (testsIterator.hasNext()) {
                    Map.Entry<String, JsonNode> testObject = testsIterator.next();
                    String testName = testObject.getKey();
                    assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                    //an array of objects
                    ArrayNode testBody = (ArrayNode) testObject.getValue();
                    testBody.forEach(arrayObject -> {
                        assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
                        ObjectNode testSection = (ObjectNode) arrayObject;
                        if (testSection.get("do") != null) {
                            ObjectNode doSection = (ObjectNode) testSection.get("do");
                            assertThat(doSection.get("headers"), CoreMatchers.notNullValue());
                            ObjectNode headersNode = (ObjectNode) doSection.get("headers");
                            LongAdder assertions = new LongAdder();
                            headers.forEach((k, v) -> {
                                assertThat(headersNode.get(k), CoreMatchers.notNullValue());
                                TextNode textNode = (TextNode) headersNode.get(k);
                                assertThat(textNode.asText(), CoreMatchers.equalTo(v));
                                assertions.increment();
                            });
                            assertThat(assertions.intValue(), CoreMatchers.equalTo(headers.size()));
                        }
                    });
                }
            });
    }


    private void assertSetupForHeaders(ObjectNode setupNode) {
        assertThat(setupNode.get("setup"), CoreMatchers.instanceOf(ObjectNode.class));
        ObjectNode setupValue = (ObjectNode) setupNode.get("setup");
        assertThat(setupValue.get("skip"), CoreMatchers.instanceOf(ObjectNode.class));
        ObjectNode skipNode = (ObjectNode) setupValue.get("skip");
        assertThat(skipNode.get("features"), CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode features = (ArrayNode) skipNode.get("features");
        List<String> featureValues = new ArrayList<>();
        features.forEach(x -> {
            if (x.isTextual()) {
                featureValues.add(x.asText());
            }
        });
        assertThat(featureValues, IsCollectionContaining.hasItem("headers"));
    }
}
