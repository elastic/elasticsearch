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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class RemoveMatchTests extends GradleUnitTestCase {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);
    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private static final boolean humanDebug = true; // useful for humans trying to debug these tests


    @Test
    public void testRemoveAll() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();



        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new RemoveMatch("_type"))
        );
        printTest(testName, transformedTests);
    }

    @Test
    public void testRemoveTest() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();



        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new RemoveMatch("_type","Basic"))
        );
        printTest(testName, transformedTests);
    }



    // only to help manually debug
    private void printTest(String testName, List<ObjectNode> tests) {
        if (humanDebug) {
            System.out.println("\n************* " + testName + " *************");
            try (SequenceWriter sequenceWriter = MAPPER.writer().writeValues(System.out)) {
                for (ObjectNode transformedTest : tests) {
                    sequenceWriter.write(transformedTest);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
