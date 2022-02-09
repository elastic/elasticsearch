/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        assertThat(csv.headers, equalTo(new String[] { "target" }));
        assertThat(csv.quote, equalTo('|'));
        assertThat(csv.separator, equalTo('/'));
        assertThat(csv.emptyValue, equalTo("empty"));
        assertThat(csv.trim, equalTo(true));
        assertThat(csv.ignoreMissing, equalTo(true));
        assertThat(properties, is(emptyMap()));
    }
}
