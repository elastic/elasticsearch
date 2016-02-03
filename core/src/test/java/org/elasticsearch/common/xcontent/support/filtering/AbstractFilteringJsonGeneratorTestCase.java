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
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

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

    private XContentBuilder newXContentBuilder(boolean inclusive, String... filters) throws IOException {
        return XContentBuilder.builder(getXContentType().xContent(), filters, inclusive);
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
     * Instanciates a new XContentBuilder with the given filters and builds a
     * sample with it.
     *
     * @param inclusive
     *            Specifies if filters are inclusive or exclusive
     */
    private XContentBuilder sample(boolean inclusive, String... filters) throws IOException {
        return sample(newXContentBuilder(inclusive, filters));
    }

    public void testNoFiltering() throws Exception {
        XContentBuilder expected = sample(true);

        assertXContentBuilder(expected, sample(true));
        assertXContentBuilder(expected, sample(true, "*"));
        assertXContentBuilder(expected, sample(true, "**"));
        assertXContentBuilder(expected, sample(false, "xyz"));
    }

    public void testNoMatch() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject().endObject();

        assertXContentBuilder(expected, sample(true, "xyz"));
        assertXContentBuilder(expected, sample(false, "*"));
        assertXContentBuilder(expected, sample(false, "**"));
    }

    public void testSimpleFieldInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject().field("title", "My awesome book").endObject();

        assertXContentBuilder(expected, sample(true, "title"));
    }

    public void testSimpleFieldExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(false, "title"));
    }


    public void testSimpleFieldWithWildcardInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "pr*"));
    }

    public void testSimpleFieldWithWildcardExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                                .field("title", "My awesome book")
                                                                .field("pages", 456)
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
                                                            .endObject();

        assertXContentBuilder(expected, sample(false, "pr*"));
    }

    public void testMultipleFieldsInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                            .field("title", "My awesome book")
                                                            .field("pages", 456)
                                                        .endObject();

        assertXContentBuilder(expected, sample(true, "title", "pages"));
    }

    public void testMultipleFieldsExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(false, "title", "pages"));
    }


    public void testSimpleArrayInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                        .startArray("tags")
                                                            .value("elasticsearch")
                                                            .value("java")
                                                        .endArray()
                                                    .endObject();

        assertXContentBuilder(expected, sample(true, "tags"));
    }

    public void testSimpleArrayExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                                .field("title", "My awesome book")
                                                                .field("pages", 456)
                                                                .field("price", 27.99)
                                                                .field("timestamp", 1428582942867L)
                                                                .nullField("default")
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

        assertXContentBuilder(expected, sample(false, "tags"));
    }


    public void testSimpleArrayOfObjectsInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "authors"));
        assertXContentBuilder(expected, sample(true, "authors.*"));
        assertXContentBuilder(expected, sample(true, "authors.*name"));
    }

    public void testSimpleArrayOfObjectsExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                                .field("title", "My awesome book")
                                                                .field("pages", 456)
                                                                .field("price", 27.99)
                                                                .field("timestamp", 1428582942867L)
                                                                .nullField("default")
                                                                .startArray("tags")
                                                                    .value("elasticsearch")
                                                                    .value("java")
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

        assertXContentBuilder(expected, sample(false, "authors"));
        assertXContentBuilder(expected, sample(false, "authors.*"));
        assertXContentBuilder(expected, sample(false, "authors.*name"));
    }

    public void testSimpleArrayOfObjectsPropertyInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
                                                            .startArray("authors")
                                                                .startObject()
                                                                    .field("lastname", "John")
                                                                .endObject()
                                                                .startObject()
                                                                    .field("lastname", "William")
                                                                .endObject()
                                                            .endArray()
                                                        .endObject();

        assertXContentBuilder(expected, sample(true, "authors.lastname"));
        assertXContentBuilder(expected, sample(true, "authors.l*"));
    }

    public void testSimpleArrayOfObjectsPropertyExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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
                        .field("firstname", "Doe")
                    .endObject()
                    .startObject()
                        .field("name", "William Smith")
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

        assertXContentBuilder(expected, sample(false, "authors.lastname"));
        assertXContentBuilder(expected, sample(false, "authors.l*"));
    }

    public void testRecurseField1Inclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "**.name"));
    }

    public void testRecurseField1Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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
                                                                        .field("lastname", "John")
                                                                        .field("firstname", "Doe")
                                                                    .endObject()
                                                                    .startObject()
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
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("street", "Hampton St")
                                                                                            .field("city", "London")
                                                                                        .endObject()
                                                                                        .startObject()
                                                                                            .field("street", "Queen St")
                                                                                            .field("city", "Stornoway")
                                                                                        .endObject()
                                                                                    .endArray()
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                        .startObject("fr")
                                                                            .field("lang", "French")
                                                                            .field("available", false)
                                                                            .startArray("distributors")
                                                                                .startObject()
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("street", "Rue Mouffetard")
                                                                                            .field("city", "Paris")
                                                                                        .endObject()
                                                                                    .endArray()
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                    .endObject()
                                                                .endObject()
                                                           .endObject();

        assertXContentBuilder(expected, sample(false, "**.name"));
    }

    public void testRecurseField2Inclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "properties.**.name"));
    }

    public void testRecurseField2Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("street", "Hampton St")
                                                                                            .field("city", "London")
                                                                                        .endObject()
                                                                                        .startObject()
                                                                                            .field("street", "Queen St")
                                                                                            .field("city", "Stornoway")
                                                                                        .endObject()
                                                                                    .endArray()
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                        .startObject("fr")
                                                                            .field("lang", "French")
                                                                            .field("available", false)
                                                                            .startArray("distributors")
                                                                                .startObject()
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("street", "Rue Mouffetard")
                                                                                            .field("city", "Paris")
                                                                                        .endObject()
                                                                                    .endArray()
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                    .endObject()
                                                                .endObject()
                                                           .endObject();

        assertXContentBuilder(expected, sample(false, "properties.**.name"));
    }


    public void testRecurseField3Inclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "properties.*.en.**.name"));
    }

    public void testRecurseField3Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("street", "Hampton St")
                                                                                            .field("city", "London")
                                                                                        .endObject()
                                                                                        .startObject()
                                                                                            .field("street", "Queen St")
                                                                                            .field("city", "Stornoway")
                                                                                        .endObject()
                                                                                    .endArray()
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

        assertXContentBuilder(expected, sample(false, "properties.*.en.**.name"));
    }


    public void testRecurseField4Inclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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

        assertXContentBuilder(expected, sample(true, "properties.**.distributors.name"));
    }

    public void testRecurseField4Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder(true).startObject()
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
                                                                            .endArray()
                                                                        .endObject()
                                                                        .startObject("fr")
                                                                            .field("lang", "French")
                                                                            .field("available", false)
                                                                            .startArray("distributors")
                                                                                .startObject()
                                                                                    .startArray("addresses")
                                                                                        .startObject()
                                                                                            .field("name", "address #1")
                                                                                            .field("street", "Rue Mouffetard")
                                                                                            .field("city", "Paris")
                                                                                        .endObject()
                                                                                    .endArray()
                                                                                .endObject()
                                                                            .endArray()
                                                                        .endObject()
                                                                    .endObject()
                                                                .endObject()
                                                           .endObject();

        assertXContentBuilder(expected, sample(false, "properties.**.distributors.name"));
    }

    public void testRawField() throws Exception {

        XContentBuilder expectedRawField = newXContentBuilder(true).startObject().field("foo", 0).startObject("raw").field("content", "hello world!").endObject().endObject();
        XContentBuilder expectedRawFieldFiltered = newXContentBuilder(true).startObject().field("foo", 0).endObject();
        XContentBuilder expectedRawFieldNotFiltered = newXContentBuilder(true).startObject().startObject("raw").field("content", "hello world!").endObject().endObject();

        BytesReference raw = newXContentBuilder(true).startObject().field("content", "hello world!").endObject().bytes();

        // Test method: rawField(String fieldName, BytesReference content)
        assertXContentBuilder(expectedRawField, newXContentBuilder(true).startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder(true, "f*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder(false, "r*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder(true, "r*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder(false, "f*").startObject().field("foo", 0).rawField("raw", raw).endObject());

        // Test method: rawField(String fieldName, InputStream content)
        assertXContentBuilder(expectedRawField, newXContentBuilder(true).startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder(true, "f*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilder(false, "r*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder(true, "r*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilder(false, "f*").startObject().field("foo", 0).rawField("raw", new ByteArrayInputStream(raw.toBytes())).endObject());
    }


    public void testArrays() throws Exception {
        // Test: Array of values (no filtering)
        XContentBuilder expected = newXContentBuilder(true).startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder(true, "t*").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder(true, "tags").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder(false, "a").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());

        // Test: Array of values (with filtering)
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(true, "foo").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(false, "t*").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(false, "tags").startObject().startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());

        // Test: Array of objects (no filtering)
        expected = newXContentBuilder(true).startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder(true, "t*").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder(true, "tags").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder(false, "a").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with filtering)
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(true, "foo").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(false, "t*").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(newXContentBuilder(true).startObject().endObject(), newXContentBuilder(false, "tags").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with partial filtering)
        expected = newXContentBuilder(true).startObject().startArray("tags").startObject().field("firstname", "ipsum").endObject().endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilder(true, "t*.firstname").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilder(false, "t*.lastname").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
    }
}
