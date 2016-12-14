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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
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
        assertThat(builder.bytes().utf8ToString(), is(expected.bytes().utf8ToString()));
    }

    protected void assertBinary(XContentBuilder expected, XContentBuilder builder) {
        assertNotNull(builder);
        assertNotNull(expected);

        try {
            XContent xContent = XContentFactory.xContent(builder.contentType());
            XContentParser jsonParser = createParser(xContent, expected.bytes());
            XContentParser testParser = createParser(xContent, builder.bytes());

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

    private XContentBuilder newXContentBuilder() throws IOException {
        return XContentBuilder.builder(getXContentType().xContent());
    }

    private XContentBuilder newXContentBuilderWithIncludes(String filter) throws IOException {
        return newXContentBuilder(singleton(filter), emptySet());
    }

    private XContentBuilder newXContentBuilderWithExcludes(String filter) throws IOException {
        return newXContentBuilder(emptySet(), singleton(filter));
    }

    private XContentBuilder newXContentBuilder(Set<String> includes, Set<String> excludes) throws IOException {
        return XContentBuilder.builder(getXContentType().xContent(), includes, excludes);
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

    /** Create a new {@link XContentBuilder} and use it to build the sample using the given inclusive filter **/
    private XContentBuilder sampleWithIncludes(String filter) throws IOException {
        return sample(newXContentBuilderWithIncludes(filter));
    }

    /** Create a new {@link XContentBuilder} and use it to build the sample using the given exclusive filter **/
    private XContentBuilder sampleWithExcludes(String filter) throws IOException {
        return sample(newXContentBuilderWithExcludes(filter));
    }

    /** Create a new {@link XContentBuilder} and use it to build the sample using the given includes and exclusive filters **/
    private XContentBuilder sampleWithFilters(Set<String> includes, Set<String> excludes) throws IOException {
        return sample(newXContentBuilder(includes, excludes));
    }

    /** Create a new {@link XContentBuilder} and use it to build the sample **/
    private XContentBuilder sample() throws IOException {
        return sample(newXContentBuilder());
    }

    public void testNoFiltering() throws Exception {
        XContentBuilder expected = sample();

        assertXContentBuilder(expected, sample());
        assertXContentBuilder(expected, sampleWithIncludes("*"));
        assertXContentBuilder(expected, sampleWithIncludes("**"));
        assertXContentBuilder(expected, sampleWithExcludes("xyz"));
    }

    public void testNoMatch() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject().endObject();

        assertXContentBuilder(expected, sampleWithIncludes("xyz"));
        assertXContentBuilder(expected, sampleWithExcludes("*"));
        assertXContentBuilder(expected, sampleWithExcludes("**"));
    }

    public void testSimpleFieldInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject().field("title", "My awesome book").endObject();

        assertXContentBuilder(expected, sampleWithIncludes("title"));
    }

    public void testSimpleFieldExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("title"));
    }

    public void testSimpleFieldWithWildcardInclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("pr*"));
    }

    public void testSimpleFieldWithWildcardExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("pr*"));
    }

    public void testMultipleFieldsInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .field("title", "My awesome book")
                                                            .field("pages", 456)
                                                        .endObject();

        assertXContentBuilder(expected, sampleWithFilters(Sets.newHashSet("title", "pages"), emptySet()));
    }

    public void testMultipleFieldsExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sample(newXContentBuilder(emptySet(), Sets.newHashSet("title", "pages"))));
    }

    public void testSimpleArrayInclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
                                                            .startArray("tags")
                                                                .value("elasticsearch")
                                                                .value("java")
                                                            .endArray()
                                                    .endObject();

        assertXContentBuilder(expected, sampleWithIncludes("tags"));
    }

    public void testSimpleArrayExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("tags"));
    }

    public void testSimpleArrayOfObjectsInclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("authors"));
        assertXContentBuilder(expected, sampleWithIncludes("authors.*"));
        assertXContentBuilder(expected, sampleWithIncludes("authors.*name"));
    }

    public void testSimpleArrayOfObjectsExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("authors"));
        assertXContentBuilder(expected, sampleWithExcludes("authors.*"));
        assertXContentBuilder(expected, sampleWithExcludes("authors.*name"));
    }

    public void testSimpleArrayOfObjectsPropertyInclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("authors.lastname"));
        assertXContentBuilder(expected, sampleWithIncludes("authors.l*"));
    }

    public void testSimpleArrayOfObjectsPropertyExclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("authors.lastname"));
        assertXContentBuilder(expected, sampleWithExcludes("authors.l*"));
    }

    public void testRecurseField1Inclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("**.name"));
    }

    public void testRecurseField1Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("**.name"));
    }

    public void testRecurseField2Inclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("properties.**.name"));
    }

    public void testRecurseField2Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("properties.**.name"));
    }

    public void testRecurseField3Inclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("properties.*.en.**.name"));
    }

    public void testRecurseField3Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("properties.*.en.**.name"));
    }

    public void testRecurseField4Inclusive() throws Exception {
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

        assertXContentBuilder(expected, sampleWithIncludes("properties.**.distributors.name"));
    }

    public void testRecurseField4Exclusive() throws Exception {
        XContentBuilder expected = newXContentBuilder().startObject()
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

        assertXContentBuilder(expected, sampleWithExcludes("properties.**.distributors.name"));
    }

    public void testRawField() throws Exception {
        XContentBuilder expectedRawField = newXContentBuilder().startObject().field("foo", 0).startObject("raw")
                .field("content", "hello world!").endObject().endObject();
        XContentBuilder expectedRawFieldFiltered = newXContentBuilder().startObject().field("foo", 0).endObject();
        XContentBuilder expectedRawFieldNotFiltered = newXContentBuilder().startObject().startObject("raw").field("content", "hello world!")
                .endObject().endObject();

        BytesReference raw = newXContentBuilder().startObject().field("content", "hello world!").endObject().bytes();

        // Test method: rawField(String fieldName, BytesReference content)
        assertXContentBuilder(expectedRawField, newXContentBuilder().startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldFiltered,
                newXContentBuilderWithIncludes("f*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldFiltered,
                newXContentBuilderWithExcludes("r*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered,
                newXContentBuilderWithIncludes("r*").startObject().field("foo", 0).rawField("raw", raw).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered,
                newXContentBuilderWithExcludes("f*").startObject().field("foo", 0).rawField("raw", raw).endObject());

        // Test method: rawField(String fieldName, InputStream content)
        assertXContentBuilder(expectedRawField,
                newXContentBuilder().startObject().field("foo", 0).rawField("raw", raw.streamInput()).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilderWithIncludes("f*").startObject().field("foo", 0)
                .rawField("raw", raw.streamInput()).endObject());
        assertXContentBuilder(expectedRawFieldFiltered, newXContentBuilderWithExcludes("r*").startObject().field("foo", 0)
                .rawField("raw", raw.streamInput()).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilderWithIncludes("r*").startObject().field("foo", 0)
                .rawField("raw", raw.streamInput()).endObject());
        assertXContentBuilder(expectedRawFieldNotFiltered, newXContentBuilderWithExcludes("f*").startObject().field("foo", 0)
                .rawField("raw", raw.streamInput()).endObject());
    }

    public void testArrays() throws Exception {
        // Test: Array of values (no filtering)
        XContentBuilder expected = newXContentBuilder().startObject().startArray("tags").value("lorem").value("ipsum").value("dolor")
                .endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilderWithIncludes("t*").startObject().startArray("tags").value("lorem").value("ipsum")
                .value("dolor").endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilderWithIncludes("tags").startObject().startArray("tags").value("lorem")
                .value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilderWithExcludes("a").startObject().startArray("tags").value("lorem").value("ipsum")
                .value("dolor").endArray().endObject());

        // Test: Array of values (with filtering)
        assertXContentBuilder(newXContentBuilder().startObject().endObject(), newXContentBuilderWithIncludes("foo").startObject()
                .startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(newXContentBuilder().startObject().endObject(), newXContentBuilderWithExcludes("t*").startObject()
                .startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());
        assertXContentBuilder(newXContentBuilder().startObject().endObject(), newXContentBuilderWithExcludes("tags").startObject()
                .startArray("tags").value("lorem").value("ipsum").value("dolor").endArray().endObject());

        // Test: Array of objects (no filtering)
        expected = newXContentBuilder().startObject().startArray("tags").startObject().field("lastname", "lorem").endObject().startObject()
                .field("firstname", "ipsum").endObject().endArray().endObject();
        assertXContentBuilder(expected, newXContentBuilderWithIncludes("t*").startObject().startArray("tags").startObject()
                .field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilderWithIncludes("tags").startObject().startArray("tags").startObject()
                .field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilderWithExcludes("a").startObject().startArray("tags").startObject()
                .field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with filtering)
        assertXContentBuilder(newXContentBuilder().startObject().endObject(),
                newXContentBuilderWithIncludes("foo").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject()
                        .startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(newXContentBuilder().startObject().endObject(),
                newXContentBuilderWithExcludes("t*").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject()
                        .startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(newXContentBuilder().startObject().endObject(),
                newXContentBuilderWithExcludes("tags").startObject().startArray("tags").startObject().field("lastname", "lorem").endObject()
                        .startObject().field("firstname", "ipsum").endObject().endArray().endObject());

        // Test: Array of objects (with partial filtering)
        expected = newXContentBuilder().startObject().startArray("tags").startObject().field("firstname", "ipsum").endObject().endArray()
                .endObject();
        assertXContentBuilder(expected, newXContentBuilderWithIncludes("t*.firstname").startObject().startArray("tags").startObject()
                .field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
        assertXContentBuilder(expected, newXContentBuilderWithExcludes("t*.lastname").startObject().startArray("tags").startObject()
                .field("lastname", "lorem").endObject().startObject().field("firstname", "ipsum").endObject().endArray().endObject());
    }

    public void testEmptyObject() throws IOException {
        final Function<XContentBuilder, XContentBuilder> build = builder -> {
            try {
                return builder.startObject().startObject("foo").endObject().endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        XContentBuilder expected = build.apply(newXContentBuilder());
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithIncludes("foo")));
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("f*"), singleton("baz"))));

        expected = newXContentBuilder().startObject().endObject();
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("foo")));
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithIncludes("bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("f*"), singleton("foo"))));
    }

    public void testSingleFieldObject() throws IOException {
        final Function<XContentBuilder, XContentBuilder> build = builder -> {
            try {
                return builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        XContentBuilder expected = build.apply(newXContentBuilder());
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithIncludes("foo.bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("foo.baz")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("foo"), singleton("foo.baz"))));

        expected = newXContentBuilder().startObject().endObject();
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("foo.bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("foo"), singleton("foo.b*"))));
    }

    public void testSingleFieldWithBothExcludesIncludes() throws IOException {
        XContentBuilder expected = newXContentBuilder()
            .startObject()
                .field("pages", 456)
                .field("price", 27.99)
            .endObject();

        assertXContentBuilder(expected, sampleWithFilters(singleton("p*"), singleton("properties")));
    }

    public void testObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("tags", "authors");
        Set<String> excludes = singleton("authors.name");

        XContentBuilder expected = newXContentBuilder()
            .startObject()
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
            .endObject();

        assertXContentBuilder(expected, sampleWithFilters(includes, excludes));
    }

    public void testRecursiveObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("**.language", "properties.weight");
        Set<String> excludes = singleton("**.distributors");

        XContentBuilder expected = newXContentBuilder()
            .startObject()
                .startObject("properties")
                    .field("weight", 0.8d)
                    .startObject("language")
                        .startObject("en")
                            .field("lang", "English")
                            .field("available", true)
                        .endObject()
                        .startObject("fr")
                            .field("lang", "French")
                            .field("available", false)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

        assertXContentBuilder(expected, sampleWithFilters(includes, excludes));
    }

    public void testRecursiveSameObjectWithBothExcludesIncludes() throws IOException {
        Set<String> includes = singleton("**.distributors");
        Set<String> excludes = singleton("**.distributors");

        XContentBuilder expected = newXContentBuilder().startObject().endObject();
        assertXContentBuilder(expected, sampleWithFilters(includes, excludes));
    }

    public void testRecursiveObjectsPropertiesWithBothExcludesIncludes() throws IOException {
        Set<String> includes = singleton("**.en.*");
        Set<String> excludes = Sets.newHashSet("**.distributors.*.name", "**.street");

        XContentBuilder expected = newXContentBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("language")
                        .startObject("en")
                            .field("lang", "English")
                            .field("available", true)
                            .startArray("distributors")
                                .startObject()
                                    .field("name", "The Book Shop")
                                    .startArray("addresses")
                                        .startObject()
                                            .field("city", "London")
                                        .endObject()
                                        .startObject()
                                            .field("city", "Stornoway")
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

        assertXContentBuilder(expected, sampleWithFilters(includes, excludes));
    }

    public void testWithLfAtEnd() throws IOException {
        final Function<XContentBuilder, XContentBuilder> build = builder -> {
            try {
                return builder.startObject().startObject("foo").field("bar", "baz").endObject().endObject().prettyPrint().lfAtEnd();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        XContentBuilder expected = build.apply(newXContentBuilder());
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithIncludes("foo")));
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("f*"), singleton("baz"))));

        expected = newXContentBuilder().startObject().endObject().prettyPrint().lfAtEnd();
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithExcludes("foo")));
        assertXContentBuilder(expected, build.apply(newXContentBuilderWithIncludes("bar")));
        assertXContentBuilder(expected, build.apply(newXContentBuilder(singleton("f*"), singleton("foo"))));
    }
}
