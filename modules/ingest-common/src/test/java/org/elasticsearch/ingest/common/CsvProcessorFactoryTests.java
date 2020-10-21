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

package org.elasticsearch.ingest.common;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CsvProcessorFactoryTests extends ESTestCase {

    public void testProcessorIsCreated() {
        CsvProcessor.Factory factory = new CsvProcessor.Factory();
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("field", "field");
        properties.put("target_fields", Collections.singletonList("target"));
        properties.put("quote", "|");
        properties.put("separator", "/");
        properties.put("empty_value", "empty");
        properties.put("trim", true);
        properties.put("ignore_missing", true);
        CsvProcessor csv = factory.create(null, "csv", null, properties);
        assertThat(csv, notNullValue());
        assertThat(csv.field, equalTo("field"));
        assertThat(csv.headers, equalTo(new String[]{"target"}));
        assertThat(csv.quote, equalTo('|'));
        assertThat(csv.separator, equalTo('/'));
        assertThat(csv.emptyValue, equalTo("empty"));
        assertThat(csv.trim, equalTo(true));
        assertThat(csv.ignoreMissing, equalTo(true));
        assertThat(properties, is(emptyMap()));
    }
}
