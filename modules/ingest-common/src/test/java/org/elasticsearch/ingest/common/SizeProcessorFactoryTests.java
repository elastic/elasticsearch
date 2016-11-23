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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class SizeProcessorFactoryTests extends ESTestCase {

    private SizeProcessor.Factory factory;

    @Before
    public void init() {
        factory = new SizeProcessor.Factory();
    }

    public void testCreateWithoutTargetField() throws Exception {
        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, Collections.emptyMap());
            fail("We should have an exception as no target is defined");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[target] required property is missing"));
        }
    }

    public void testCreateSetTargetField() throws Exception {
        String fieldName = randomAsciiOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("target", fieldName);
        String processorTag = randomAsciiOfLength(10);
        SizeProcessor sizeProcessor = factory.create(null, processorTag, config);
        assertThat(sizeProcessor.getTag(), equalTo(processorTag));
        assertThat(sizeProcessor.getTarget(), equalTo(fieldName));
    }
}
