/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.warnings;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.test.rest.transform.feature.InjectFeatureTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class InjectWarningsTests extends InjectFeatureTests {

List<String> addWarnings = List.of("added warning");

    /**
     * test file does not any headers defined
     */
    @Test
    public void testInjectHeadersNoPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/without_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
      //  validateBodyHasHeaders(transformedTests, addWarnings);
    }

    /**
     * test file has preexisting headers
     */
    @Test
    public void testInjectHeadersWithPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
    //    validateBodyHasHeaders(tests, Map.of("foo", "bar"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    //    validateBodyHasHeaders(tests, Map.of("foo", "bar"));
        //validateBodyHasHeaders(transformedTests, addWarnings);
    }

    @Override
    protected List<String> getKnownFeatures() {
        return Collections.singletonList("warnings");
    }

    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new InjectWarnings(addWarnings));
    }

    @Override
    protected boolean getHumanDebug() {
        return true;
    }

//    private void validateBodyHasHeaders(List<ObjectNode> tests, Map<String, String> headers) {
//        tests.forEach(test -> {
//            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
//            while (testsIterator.hasNext()) {
//                Map.Entry<String, JsonNode> testObject = testsIterator.next();
//                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
//                ArrayNode testBody = (ArrayNode) testObject.getValue();
//                testBody.forEach(arrayObject -> {
//                    assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
//                    ObjectNode testSection = (ObjectNode) arrayObject;
//                    if (testSection.get("do") != null) {
//                        ObjectNode doSection = (ObjectNode) testSection.get("do");
//                        assertThat(doSection.get("headers"), CoreMatchers.notNullValue());
//                        ObjectNode headersNode = (ObjectNode) doSection.get("headers");
//                        LongAdder assertions = new LongAdder();
//                        headers.forEach((k, v) -> {
//                            assertThat(headersNode.get(k), CoreMatchers.notNullValue());
//                            TextNode textNode = (TextNode) headersNode.get(k);
//                            assertThat(textNode.asText(), CoreMatchers.equalTo(v));
//                            assertions.increment();
//                        });
//                        assertThat(assertions.intValue(), CoreMatchers.equalTo(headers.size()));
//                    }
//                });
//            }
//        });
//    }
}
