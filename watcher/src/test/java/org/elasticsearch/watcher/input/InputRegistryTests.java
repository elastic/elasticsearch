/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class InputRegistryTests extends ESTestCase {

    @Test(expected = ElasticsearchParseException.class)
    public void testParse_EmptyInput() throws Exception {
        InputRegistry registry = new InputRegistry(ImmutableMap.<String, InputFactory>of());
        XContentParser parser = JsonXContent.jsonXContent.createParser(
                jsonBuilder().startObject().endObject().bytes());
        parser.nextToken();
        registry.parse("_id", parser);
        fail("expecting an exception when trying to parse an empty input");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParse_ArrayInput() throws Exception {
        InputRegistry registry = new InputRegistry(ImmutableMap.<String, InputFactory>of());
        XContentParser parser = JsonXContent.jsonXContent.createParser(
                jsonBuilder().startArray().endArray().bytes());
        parser.nextToken();
        registry.parse("_id", parser);
        fail("expecting an exception when trying to parse an input that is not an object");
    }
}
