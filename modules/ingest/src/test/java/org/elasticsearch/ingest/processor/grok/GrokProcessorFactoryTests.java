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

package org.elasticsearch.ingest.processor.grok;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GrokProcessorFactoryTests extends ESTestCase {

    private Path configDir;

    @Before
    public void prepareConfigDirectory() throws Exception {
        this.configDir = createTempDir();
        Path grokDir = configDir.resolve("ingest").resolve("grok");
        Path patternsDir = grokDir.resolve("patterns");
        Files.createDirectories(patternsDir);
    }

    public void testBuild() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(configDir);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("pattern", "(?<foo>\\w+)");
        GrokProcessor processor = factory.create(config);
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
    }

    public void testCreateWithCustomPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(configDir);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("pattern", "%{MY_PATTERN:name}!");
        config.put("pattern_definitions", Collections.singletonMap("MY_PATTERN", "foo"));
        GrokProcessor processor = factory.create(config);
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.getGrok().match("foo!"), equalTo(true));
    }
}
