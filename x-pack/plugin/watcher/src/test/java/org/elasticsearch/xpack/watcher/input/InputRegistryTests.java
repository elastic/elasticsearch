/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class InputRegistryTests extends ESTestCase {

    public void testParseEmptyInput() throws Exception {
        InputRegistry registry = new InputRegistry(emptyMap());
        XContentParser parser = createParser(jsonBuilder().startObject().endObject());
        parser.nextToken();
        try {
            registry.parse("_id", parser);
            fail("expecting an exception when trying to parse an empty input");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected field indicating the input type, but found an empty object instead"));
        }
    }

    public void testParseArrayInput() throws Exception {
        InputRegistry registry = new InputRegistry(emptyMap());
        XContentParser parser = createParser(jsonBuilder().startArray().endArray());
        parser.nextToken();
        try {
            registry.parse("_id", parser);
            fail("expecting an exception when trying to parse an input that is not an object");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an object representing the input, but found [START_ARRAY] instead"));
        }
    }
}
