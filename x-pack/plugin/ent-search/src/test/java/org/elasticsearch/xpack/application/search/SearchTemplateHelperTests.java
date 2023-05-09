/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import com.fasterxml.jackson.core.JsonParseException;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class SearchTemplateHelperTests extends ESTestCase {
    public void testStringArrayWithTrailingComma() throws IOException {
        String json = """
            {
                 "key": ["1", "2",]
            }
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            {
                 "key": ["1", "2"]
            }
            """;
        assertJsonEquals(cleanJson, expected);
        ;
    }

    public void testNumberArrayWithTrailingComma() throws IOException {
        String json = """
            {
                 "key": [1, 2,]
            }
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            {
                 "key": [1, 2]
            }
            """;
        assertJsonEquals(cleanJson, expected);
        ;
    }

    public void testObjectArrayWithTrailingComma() throws IOException {
        String json = """
            {
                 "key": [
                     { "sub": 3 },
                     { "sub": 4 },
                 ]
            }
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            {
                 "key": [
                     { "sub": 3 },
                     { "sub": 4 }
                 ]
            }
            """;
        assertJsonEquals(cleanJson, expected);
    }

    public void testMapWithTrailingComma() throws IOException {
        String json = """
            {
                 "key1": "value1",
                 "key2": "value2",
            }
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            {
                 "key1": "value1",
                 "key2": "value2"
            }
            """;
        assertJsonEquals(cleanJson, expected);
    }

    public void testInnerMapWithTrailingComma() throws IOException {
        String json = """
            {
                 "key1": "value1",
                 "key2": {
                     "test1": 1,
                     "test2": 1,
                 }
            }
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            {
                 "key1": "value1",
                 "key2": {
                     "test1": 1,
                     "test2": 1
                 }
            }
            """;
        assertJsonEquals(cleanJson, expected);
    }

    public void testArrayWithTrailingComma() throws IOException {
        String json = """
            ["key1", "value1",]
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            ["key1", "value1"]
            """;
        assertJsonEquals(cleanJson, expected);
    }

    public void testEndArrayWithTrailingComma() throws IOException {
        String json = """
            ["key1", "value1",],
            """;
        String cleanJson = SearchTemplateHelper.stripTrailingComma(json);
        String expected = """
            ["key1", "value1"]
            """;
        assertJsonEquals(cleanJson, expected);
    }

    public void testInvalidJson() throws IOException {
        String json = """
            {
                "key"
            }
            """;
        expectThrows(JsonParseException.class, () -> SearchTemplateHelper.stripTrailingComma(json));
    }

    void assertJsonEquals(String expected, String res) throws IOException {
        assertThat(XContentHelper.stripWhitespace(res), equalTo(XContentHelper.stripWhitespace(expected)));
    }
}
