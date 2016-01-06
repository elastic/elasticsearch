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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class ConvertProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("type", type.toString());
        ConvertProcessor convertProcessor = factory.create(config);
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
    }

    public void testCreateUnsupportedType() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAsciiOfLengthBetween(1, 10);
        config.put("field", "field1");
        config.put("type", type);
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.equalTo("type [" + type + "] not supported, cannot convert field."));
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAsciiOfLengthBetween(1, 10);
        config.put("type", type);
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.equalTo("required property [field] is missing"));
        }
    }

    public void testCreateNoTypePresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.equalTo("required property [type] is missing"));
        }
    }
}
