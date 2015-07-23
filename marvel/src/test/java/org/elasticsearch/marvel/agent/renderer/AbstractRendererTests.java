/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.nullValue;

@Ignore
public abstract class AbstractRendererTests extends ElasticsearchTestCase {

    @Test
    public void testSample() throws IOException {
        logger.debug("--> creating the sample document");
        MarvelDoc marvelDoc = newMarvelDoc();
        assertNotNull(marvelDoc);

        logger.debug("--> creating the renderer");
        Renderer renderer = newRenderer();
        assertNotNull(renderer);

        try (BytesStreamOutput os = new BytesStreamOutput()) {
            logger.debug("--> rendering the document");
            renderer.render(marvelDoc, XContentType.JSON, os);

            String sample = sampleFilePath();
            assertTrue(Strings.hasText(sample));

            logger.debug("--> loading expected document from file {}", sample);
            String expected = Streams.copyToStringFromClasspath(sample);

            logger.debug("--> comparing both document, they must be identical");
            assertContent(os.bytes(), expected);
        }
    }

    protected abstract Renderer newRenderer();

    protected abstract MarvelDoc newMarvelDoc();

    protected abstract String sampleFilePath();

    protected void assertContent(BytesReference result, String expected) {
        assertNotNull(result);
        assertNotNull(expected);

        try {
            XContentParser resultParser = XContentFactory.xContent(result).createParser(result);
            XContentParser expectedParser = XContentFactory.xContent(expected).createParser(expected);

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
                        assertThat(resultParser.currentName(), Matchers.equalTo(expectedParser.currentName()));
                        break;
                    case VALUE_STRING:
                        assertThat(resultParser.text(), Matchers.equalTo(expectedParser.text()));
                        break;
                    case VALUE_NUMBER:
                        assertThat(resultParser.numberType(), Matchers.equalTo(expectedParser.numberType()));
                        assertThat(resultParser.numberValue(), Matchers.equalTo(expectedParser.numberValue()));
                        break;
                }
            }
        } catch (Exception e) {
            fail("Fail to verify the result of the renderer: " + e.getMessage());
        }
    }
}
