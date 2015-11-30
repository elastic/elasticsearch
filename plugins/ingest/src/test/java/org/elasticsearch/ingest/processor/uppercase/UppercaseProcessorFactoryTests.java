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

package org.elasticsearch.ingest.processor.uppercase;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class UppercaseProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        UppercaseProcessor.Factory factory = new UppercaseProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<String> fields = Collections.singletonList("field1");
        config.put("fields", fields);
        UppercaseProcessor uppercaseProcessor = factory.create(config);
        assertThat(uppercaseProcessor.getFields(), equalTo(fields));
    }

    public void testCreateMissingFields() throws Exception {
        UppercaseProcessor.Factory factory = new UppercaseProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("required property [fields] is missing"));
        }
    }
}
