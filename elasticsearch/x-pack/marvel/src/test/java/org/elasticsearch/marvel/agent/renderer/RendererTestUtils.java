/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RendererTestUtils {

    /**
     * Verifies that two JSON documents have the same structure/fields once parsed.
     * This method does not verify that fields values are identical.
     */
    public static void assertJSONStructure(String result, String expected) {
        assertContent(result, expected, false);
    }

    /**
     * Verifies that two JSON documents are identical once parsed.
     * This method verifies that fields values are the same.
     */
    public static void assertJSONStructureAndValues(String result, String expected) {
        assertContent(result, expected, false);
    }

    private static void assertContent(String result, String expected, boolean verifyValues) {
        assertNotNull(result);
        assertNotNull(expected);

        try (
            XContentParser resultParser = XContentFactory.xContent(result).createParser(result);
            XContentParser expectedParser = XContentFactory.xContent(expected).createParser(expected);
        ) {
            while (true) {
                XContentParser.Token token1 = resultParser.nextToken();
                XContentParser.Token token2 = expectedParser.nextToken();
                if (token1 == null) {
                    assertThat(token2, nullValue());
                    return;
                }
                assertThat(token1, Matchers.equalTo(token2));
                switch (token1) {
                    case FIELD_NAME:
                        assertThat("field name for property '" + resultParser.currentName() + "' must be identical",
                                resultParser.currentName(), Matchers.equalTo(expectedParser.currentName()));
                        break;
                    case VALUE_STRING:
                        if (verifyValues) {
                            assertThat("string value for property '" + resultParser.currentName() + "' must be identical",
                                    resultParser.text(), Matchers.equalTo(expectedParser.text()));
                        } else {
                            assertThat("string value for property '" + resultParser.currentName() + "' must be empty or non empty",
                                    Strings.hasLength(resultParser.text()), Matchers.equalTo(Strings.hasLength(expectedParser.text())));
                        }
                        break;
                    case VALUE_NUMBER:
                        if (verifyValues) {
                            assertThat("numeric value for property '" + resultParser.currentName() + "' must be identical",
                                    resultParser.numberValue(), Matchers.equalTo(expectedParser.numberValue()));
                        }
                        break;
                }
            }
        } catch (Exception e) {
            fail("Fail to verify the result of the renderer: " + e.getMessage());
        }
    }

    public static String renderAsJSON(MarvelDoc marvelDoc, Renderer renderer) throws IOException {
        assertNotNull(marvelDoc);
        assertNotNull(renderer);

        try (BytesStreamOutput os = new BytesStreamOutput()) {
            renderer.render(marvelDoc, XContentType.JSON, os);
            return os.bytes().toUtf8();
        }
    }
}
