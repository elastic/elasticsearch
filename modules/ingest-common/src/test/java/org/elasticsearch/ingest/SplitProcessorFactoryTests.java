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

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class SplitProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        String processorTag = randomAsciiOfLength(10);
        config.put(AbstractProcessorFactory.TAG_KEY, processorTag);
        SplitProcessor splitProcessor = factory.create(config);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), equalTo("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
    }

    public void testCreateNoFieldPresent() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", "\\.");
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoSeparatorPresent() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[separator] required property is missing"));
        }
    }
}
