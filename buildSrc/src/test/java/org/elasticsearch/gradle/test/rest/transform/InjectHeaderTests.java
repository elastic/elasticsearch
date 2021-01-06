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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InjectHeaderTests extends GradleUnitTestCase {

    private static final YAMLFactory yaml = new YAMLFactory();
    private static final ObjectMapper mapper = new ObjectMapper(yaml);


    @Test
    public void testInjectHeadersWithoutSetupBlock() throws Exception {

        File testFile = new File(getClass().getResource("/rest/header_inject_no_setup.yml").toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        List<Map<String, String>> headers = List.of(
            Map.of("Content-Type", "application/vnd.elasticsearch+json;compatible-with=7"),
            Map.of("Accept", "application/vnd.elasticsearch+json;compatible-with=7")
        );

        transformer.transformRestTests(tests,  Collections.singletonList(new InjectHeaders(headers)));

    }
}
