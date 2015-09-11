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

package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractFilteringJsonGeneratorTestCase extends ESTestCase {

    protected abstract XContentType getXContentType();

    protected abstract void assertXContentBuilder(XContentBuilder expected, XContentBuilder builder);

    protected void assertString(XContentBuilder expected, XContentBuilder builder) {
        assertNotNull(builder);
        assertNotNull(expected);

        // Verify that the result is equal to the expected string
        assertThat(builder.bytes().toUtf8(), is(expected.bytes().toUtf8()));
    }

    protected void assertBinary(XContentBuilder expected, XContentBuilder builder) {
        assertNotNull(builder);
        assertNotNull(expected);

        try {
            XContent xContent = XContentFactory.xContent(builder.contentType());
            XContentParser jsonParser = xContent.createParser(expected.bytes());
            XContentParser testParser = xContent.createParser(builder.bytes());

            while (true) {
                XContentParser.Token token1 = jsonParser.nextToken();
                XContentParser.Token token2 = testParser.nextToken();
                if (token1 == null) {
                    assertThat(token2, nullValue());
                    return;
                }
                assertThat(token1, equalTo(token2));
                switch (token1) {
                    case FIELD_NAME:
                        assertThat(jsonParser.currentName(), equalTo(testParser.currentName()));
                        break;
                    case VALUE_STRING:
                        assertThat(jsonParser.text(), equalTo(testParser.text()));
                        break;
                    case VALUE_NUMBER:
                        assertThat(jsonParser.numberType(), equalTo(testParser.numberType()));
                        assertThat(jsonParser.numberValue(), equalTo(testParser.numberValue()));
                        break;
                }
            }
        } catch (Exception e) {
            fail("Fail to verify the result of the XContentBuilder: " + e.getMessage());
        }
    }

    private XContentBuilder newXContentBuilder(String... filters) throws IOException {
        return XContentBuilder.builder(getXContentType().xContent(), filters);
    }

    /**
     * Build a sample using a given XContentBuilder
     */
    private XContentBuilder sample(XContentBuilder builder) throws IOException {
        assertNotNull(builder);
        builder.startObject()
                .field("title", "My awesome book")
                .field("pages", 456)
                .field("price", 27.99)
                .field("timestamp", 1428582942867L)
                .nullField("default")
                .startArray("tags")
                    .value("elasticsearch")
                    .value("java")
                .endArray()
                .startArray("authors")
                    .startObject()
                        .field("name", "John Doe")
                        .field("lastname", "John")
                        .field("firstname", "Doe")
                    .endObject()
                    .startObject()
                        .field("name", "William Smith")
                        .field("lastname", "William")
                        .field("firstname", "Smith")
                    .endObject()
                .endArray()
                .startObject("properties")
                    .field("weight", 0.8d)
                    .startObject("language")
                        .startObject("en")
                            .field("lang", "English")
                            .field("available", true)
                            .startArray("distributors")
                                .startObject()
                                    .field("name", "The Book Shop")
                                    .startArray("addresses")
                                        .startObject()
                                            .field("name", "address #1")
                                            .field("street", "Hampton St")
                                            .field("city", "London")
                                        .endObject()
                                        .startObject()
                                            .field("name", "address #2")
                                            .field("street", "Queen St")
                                            .field("city", "Stornoway")
                                        .endObject()
                                    .endArray()
                                .endObject()
                                .startObject()
                                    .field("name", "Sussex Books House")
                                .endObject()
                            .endArray()
                        .endObject()
                        .startObject("fr")
                            .field("lang", "French")
                            .field("available", false)
                            .startArray("distributors")
                                .startObject()
                                    .field("name", "La Maison du Livre")
                                    .startArray("addresses")
                                        .startObject()
                                            .field("name", "address #1")
                                            .field("street", "Rue Mouffetard")
                                            .field("city", "Paris")
                                        .endObject()
                                    .endArray()
                                .endObject()
                                .startObject()
                                    .field("name", "Thetra")
                                .endObject()
                            .endArray()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        return builder;
    }

    /**
     * Instanciates a new XContentBuilder with the given filters and builds a sample with it.
     */
    private XContentBuilder sample(String... filters) throws IOException {
        return sample(newXContentBuilder(filters));
    }

    @Test
    public void testNoFiltering() throws Exception {
        XContentBuilder expected = sample();

        assertXContentBuilder(expected, sample());
        assertXContentBuilder(expected, sample("*"));
        assertXContentBuilder(expected, sample("**"));
    }

    @Test
    public void testNoMatch() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject().endObject();

        assertXContentBuilder(expected, sample("xyz"));
    }

    @Test
    public void testSimpleField() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .field("title", "My awesome book")
                                                        .endObject();

        assertXContentBuilder(expected, sample("title"));
    }

    @Test
    public void testSimpleFieldWithWildcard() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .field("price", 27.99)
                                                            .startObject("properties")
                                                                .field("weight", 0.8d)
                                                                .startObject("language")
                                                                    .startObject("en")
                                                                        .field("lang", "English")
                                                                        .field("available", true)
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "The Book Shop")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                        .field("street", "Hampton St")
                                                                                        .field("city", "London")
                                                                                    .endObject()
                                                                                    .startObject()
                                                                                        .field("name", "address #2")
                                                                                        .field("street", "Queen St")
                                                                                        .field("city", "Stornoway")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Sussex Books House")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                    .startObject("fr")
                                                                        .field("lang", "French")
                                                                        .field("available", false)
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "La Maison du Livre")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                        .field("street", "Rue Mouffetard")
                                                                                        .field("city", "Paris")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Thetra")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                .endObject()
                                                            .endObject()
                                                        .endObject();

        assertXContentBuilder(expected, sample("pr*"));
    }

    @Test
    public void testMultipleFields() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .field("title", "My awesome book")
                                                            .field("pages", 456)
                                                        .endObject();

        assertXContentBuilder(expected, sample("title", "pages"));
    }

    @Test
    public void testSimpleArray() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                        .startArray("tags")
                                                            .value("elasticsearch")
                                                            .value("java")
                                                        .endArray()
                                                    .endObject();

        assertXContentBuilder(expected, sample("tags"));
    }

    @Test
    public void testSimpleArrayOfObjects() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                        .startArray("authors")
                                                            .startObject()
                                                                .field("name", "John Doe")
                                                                .field("lastname", "John")
                                                                .field("firstname", "Doe")
                                                            .endObject()
                                                            .startObject()
                                                                .field("name", "William Smith")
                                                                .field("lastname", "William")
                                                                .field("firstname", "Smith")
                                                            .endObject()
                                                        .endArray()
                                                    .endObject();

        assertXContentBuilder(expected, sample("authors"));
        assertXContentBuilder(expected, sample("authors.*"));
        assertXContentBuilder(expected, sample("authors.*name"));
    }

    @Test
    public void testSimpleArrayOfObjectsProperty() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .startArray("authors")
                                                                .startObject()
                                                                    .field("lastname", "John")
                                                                .endObject()
                                                                .startObject()
                                                                    .field("lastname", "William")
                                                                .endObject()
                                                            .endArray()
                                                        .endObject();

        assertXContentBuilder(expected, sample("authors.lastname"));
        assertXContentBuilder(expected, sample("authors.l*"));
    }

    @Test
    public void testRecurseField1() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .startArray("authors")
                                                                .startObject()
                                                                    .field("name", "John Doe")
                                                                .endObject()
                                                                .startObject()
                                                                    .field("name", "William Smith")
                                                            .   endObject()
                                                            .endArray()
                                                            .startObject("properties")
                                                                .startObject("language")
                                                                    .startObject("en")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "The Book Shop")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                    .endObject()
                                                                                    .startObject()
                                                                                        .field("name", "address #2")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Sussex Books House")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                    .startObject("fr")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "La Maison du Livre")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Thetra")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                .endObject()
                                                            .endObject()
                                                        .endObject();

        assertXContentBuilder(expected, sample("**.name"));
    }

    @Test
    public void testRecurseField2() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .startObject("properties")
                                                                .startObject("language")
                                                                    .startObject("en")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "The Book Shop")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                    .endObject()
                                                                                    .startObject()
                                                                                        .field("name", "address #2")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Sussex Books House")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                    .startObject("fr")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "La Maison du Livre")
                                                                                .startArray("addresses")
                                                                                    .startObject()
                                                                                        .field("name", "address #1")
                                                                                    .endObject()
                                                                                .endArray()
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Thetra")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                .endObject()
                                                            .endObject()
                                                        .endObject();

        assertXContentBuilder(expected, sample("properties.**.name"));
    }

    @Test
    public void testRecurseField3() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                        .startObject("properties")
                                                            .startObject("language")
                                                                .startObject("en")
                                                                    .startArray("distributors")
                                                                        .startObject()
                                                                            .field("name", "The Book Shop")
                                                                            .startArray("addresses")
                                                                                .startObject()
                                                                                    .field("name", "address #1")
                                                                                .endObject()
                                                                                .startObject()
                                                                                    .field("name", "address #2")
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                        .startObject()
                                                                            .field("name", "Sussex Books House")
                                                                        .endObject()
                                                                    .endArray()
                                                                .endObject()
                                                            .endObject()
                                                        .endObject()
                                                    .endObject();

        assertXContentBuilder(expected, sample("properties.*.en.**.name"));
    }

    @Test
    public void testRecurseField4() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .startObject("properties")
                                                                .startObject("language")
                                                                    .startObject("en")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "The Book Shop")
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Sussex Books House")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                    .startObject("fr")
                                                                        .startArray("distributors")
                                                                            .startObject()
                                                                                .field("name", "La Maison du Livre")
                                                                            .endObject()
                                                                            .startObject()
                                                                                .field("name", "Thetra")
                                                                            .endObject()
                                                                        .endArray()
                                                                    .endObject()
                                                                .endObject()
                                                            .endObject()
                                                        .endObject();

        assertXContentBuilder(expected, sample("properties.**.distributors.name"));
    }

    @Test
    public void testRawField() throws Exception {

        XContentBuilder expectedRawField = newXContentBuilder().startObject().field("foo", 0).startObject("raw").field("content", "hello world!").endObject().endObject();
        XContentBuilder expectedRawFieldFiltered = newXContentBuilder().startObject().field("foo", 0).endObject();
        XContentBuilder expectedRawFieldNotFiltered =newXContentBuilder().startObject().startObject("raw").field("content", "hello world!").endObject().endObject();

        BytesReference raw = newXContentBuilder().startObject().field("content", "hello world!").endObject().bytes();

        // Test method: rawField(String fieldName, BytesReference content)
        assertXContentBuilder(expectedRawField, newXContentBuilder().startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder("f*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder("r*").startObject().field("foo", 0).rawField("raw", raw).endObject());

        // Test method: rawField(String fieldName, byte[] content)
        assertXContentBuilder(expectedRawField, newXContentBuilder().startObject().field("foo", 0).rawField("raw", raw.toBytes()).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder("f*").startObject().field("foo", 0).rawField("raw", raw.toBytes()).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder("r*").startObject().field("foo", 0).rawField("raw", raw.toBytes()).endObject());

        // Test method: rawField(String fieldName, InputStream content)
        assertXContentBuilder(expectedRawField, newXContentBuilder().startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder("f*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder("r*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
    }

    @Test
    public void testArrays() throws Exception {
        // Test: Array of values (no filtering)
        XContentBuilder expected = newXContentBuilder().startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder("t*").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder("tags").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());

        // Test: Array of values (with filtering)
        assertXContentBuilder(newXContentBuilder().startObject().endObject(), newXContentBuilder("foo").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());

        // Test: Array of objects (no filtering)
        expected = newXContentBuilder().startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder("t*").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder("tags").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with filtering)
        assertXContentBuilder(newXContentBuilder().startObject().endObject(), newXContentBuilder("foo").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with partial filtering)
        expected = newXContentBuilder().startObject().startArray("tags").startObject().field("firstname", "ipsum").endObject().endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder("t*.firstname").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

    }
}
