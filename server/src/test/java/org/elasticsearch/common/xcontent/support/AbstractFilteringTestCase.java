/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

/**
 * Tests for {@link XContent} filtering.
 */
public abstract class AbstractFilteringTestCase extends ESTestCase {

    @FunctionalInterface
    protected interface Builder extends CheckedFunction<XContentBuilder, XContentBuilder, IOException> {
    }

    protected abstract void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException;

    /** Sample test case **/
    protected static final Builder SAMPLE = builder -> builder.startObject()
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

    public void testNoFiltering() throws Exception {
        final Builder expected = SAMPLE;

        testFilter(expected, SAMPLE, emptySet(), emptySet());
        testFilter(expected, SAMPLE, singleton("*"), emptySet());
        testFilter(expected, SAMPLE, singleton("**"), emptySet());
        testFilter(expected, SAMPLE, emptySet(), singleton("xyz"));
    }

    public void testNoMatch() throws Exception {
        final Builder expected = builder -> builder.startObject().endObject();

        testFilter(expected, SAMPLE, singleton("xyz"), emptySet());
        testFilter(expected, SAMPLE, emptySet(), singleton("*"));
        testFilter(expected, SAMPLE, emptySet(), singleton("**"));
    }

    public void testSimpleFieldInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject().field("title", "My awesome book").endObject();

        testFilter(expected, SAMPLE, singleton("title"), emptySet());
    }

    public void testSimpleFieldExclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("title"));
    }

    public void testSimpleFieldWithWildcardInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("pr*"), emptySet());
    }

    public void testSimpleFieldWithWildcardExclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("pr*"));
    }

    public void testMultipleFieldsInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
                                                            .field("title", "My awesome book")
                                                            .field("pages", 456)
                                                        .endObject();

        testFilter(expected, SAMPLE, Sets.newHashSet("title", "pages"), emptySet());
    }

    public void testMultipleFieldsExclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), Sets.newHashSet("title", "pages"));
    }

    public void testSimpleArrayInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
                                                            .startArray("tags")
                                                                .value("elasticsearch")
                                                                .value("java")
                                                            .endArray()
                                                    .endObject();

        testFilter(expected, SAMPLE, singleton("tags"), emptySet());
    }

    public void testSimpleArrayExclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("tags"));
    }

    public void testSimpleArrayOfObjectsInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("authors"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.*"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.*name"), emptySet());
    }

    protected static final Builder SIMPLE_ARRAY_OF_OBJECTS_EXCLUSIVE = builder -> builder.startObject()
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

    public void testSimpleArrayOfObjectsExclusive() throws Exception {
        testFilter(SIMPLE_ARRAY_OF_OBJECTS_EXCLUSIVE, SAMPLE, emptySet(), singleton("authors"));
        testFilter(SIMPLE_ARRAY_OF_OBJECTS_EXCLUSIVE, SAMPLE, emptySet(), singleton("authors.*"));
        testFilter(SIMPLE_ARRAY_OF_OBJECTS_EXCLUSIVE, SAMPLE, emptySet(), singleton("authors.*name"));
    }

    public void testSimpleArrayOfObjectsPropertyInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
                                                            .startArray("authors")
                                                                .startObject()
                                                                    .field("lastname", "John")
                                                                .endObject()
                                                                .startObject()
                                                                    .field("lastname", "William")
                                                                .endObject()
                                                            .endArray()
                                                        .endObject();

        testFilter(expected, SAMPLE, singleton("authors.lastname"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.l*"), emptySet());
    }

    public void testSimpleArrayOfObjectsPropertyExclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("authors.lastname"));
        testFilter(expected, SAMPLE, emptySet(), singleton("authors.l*"));
    }

    public void testRecurseField1Inclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("**.name"), emptySet());
    }

    public void testRecurseField1Exclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("**.name"));
    }

    public void testRecurseField2Inclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("properties.**.name"), emptySet());
    }

    public void testRecurseField2Exclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("properties.**.name"));
    }

    public void testRecurseField3Inclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("properties.*.en.**.name"), emptySet());
    }

    public void testRecurseField3Exclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("properties.*.en.**.name"));
    }

    public void testRecurseField4Inclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, singleton("properties.**.distributors.name"), emptySet());
    }

    public void testRecurseField4Exclusive() throws Exception {
        final Builder expected = builder -> builder.startObject()
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

        testFilter(expected, SAMPLE, emptySet(), singleton("properties.**.distributors.name"));
    }

    public void testRawField() throws Exception {
        final Builder expectedRawField = builder -> builder
                .startObject()
                    .field("foo", 0)
                    .startObject("raw")
                        .field("content", "hello world!")
                    .endObject()
                .endObject();

        final Builder expectedRawFieldFiltered = builder -> builder
                .startObject()
                    .field("foo", 0)
                .endObject();

        final Builder expectedRawFieldNotFiltered = builder -> builder
                .startObject()
                    .startObject("raw")
                        .field("content", "hello world!")
                    .endObject()
                .endObject();

        Builder sampleWithRaw = builder -> {
            BytesReference raw = BytesReference
                    .bytes(XContentBuilder.builder(builder.contentType().xContent())
                            .startObject()
                                .field("content", "hello world!")
                            .endObject());
            return builder.startObject().field("foo", 0).rawField("raw", raw.streamInput()).endObject();
        };

        // Test method: rawField(String fieldName, BytesReference content)
        testFilter(expectedRawField, sampleWithRaw, emptySet(), emptySet());
        testFilter(expectedRawFieldFiltered, sampleWithRaw, singleton("f*"), emptySet());
        testFilter(expectedRawFieldFiltered, sampleWithRaw, emptySet(), singleton("r*"));
        testFilter(expectedRawFieldNotFiltered, sampleWithRaw, singleton("r*"), emptySet());
        testFilter(expectedRawFieldNotFiltered, sampleWithRaw, emptySet(), singleton("f*"));

        sampleWithRaw = builder -> {
            BytesReference raw = BytesReference
                    .bytes(XContentBuilder.builder(builder.contentType().xContent())
                            .startObject()
                            .   field("content", "hello world!")
                            .endObject());
            return builder.startObject().field("foo", 0).rawField("raw", raw.streamInput()).endObject();
        };

        // Test method: rawField(String fieldName, InputStream content)
        testFilter(expectedRawField, sampleWithRaw, emptySet(), emptySet());
        testFilter(expectedRawFieldFiltered, sampleWithRaw, singleton("f*"), emptySet());
        testFilter(expectedRawFieldFiltered, sampleWithRaw, emptySet(), singleton("r*"));
        testFilter(expectedRawFieldNotFiltered, sampleWithRaw, singleton("r*"), emptySet());
        testFilter(expectedRawFieldNotFiltered, sampleWithRaw, emptySet(), singleton("f*"));
    }

    public void testArrays() throws Exception {
        // Test: Array of values (no filtering)
        final Builder sampleArrayOfValues = builder -> builder
                .startObject()
                    .startArray("tags")
                        .value("lorem").value("ipsum").value("dolor")
                    .endArray()
                .endObject();
        testFilter(sampleArrayOfValues, sampleArrayOfValues, singleton("t*"), emptySet());
        testFilter(sampleArrayOfValues, sampleArrayOfValues, singleton("tags"), emptySet());
        testFilter(sampleArrayOfValues, sampleArrayOfValues, emptySet(), singleton("a"));

        // Test: Array of values (with filtering)
        Builder expected = builder -> builder.startObject().endObject();
        testFilter(expected, sampleArrayOfValues, singleton("foo"), emptySet());
        testFilter(expected, sampleArrayOfValues, emptySet(), singleton("t*"));
        testFilter(expected, sampleArrayOfValues, emptySet(), singleton("tags"));

        // Test: Array of objects (no filtering)
        final Builder sampleArrayOfObjects = builder -> builder
                .startObject()
                    .startArray("tags")
                        .startObject()
                            .field("lastname", "lorem")
                        .endObject()
                        .startObject()
                            .field("firstname", "ipsum")
                        .endObject()
                    .endArray()
                .endObject();
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, singleton("t*"), emptySet());
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, singleton("tags"), emptySet());
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, emptySet(), singleton("a"));

        // Test: Array of objects (with filtering)
        testFilter(expected, sampleArrayOfObjects, singleton("foo"), emptySet());
        testFilter(expected, sampleArrayOfObjects, emptySet(), singleton("t*"));
        testFilter(expected, sampleArrayOfObjects, emptySet(), singleton("tags"));

        // Test: Array of objects (with partial filtering)
        expected = builder -> builder
                .startObject()
                    .startArray("tags")
                        .startObject()
                            .field("firstname", "ipsum")
                        .endObject()
                    .endArray()
                .endObject();
        testFilter(expected, sampleArrayOfObjects, singleton("t*.firstname"), emptySet());
        testFilter(expected, sampleArrayOfObjects, emptySet(), singleton("t*.lastname"));
    }

    public void testEmptyObject() throws IOException {
        final Builder sample = builder -> builder.startObject().startObject("foo").endObject().endObject();

        Builder expected = builder -> builder.startObject().startObject("foo").endObject().endObject();
        testFilter(expected, sample, singleton("foo"), emptySet());
        testFilter(expected, sample, emptySet(), singleton("bar"));
        testFilter(expected, sample, singleton("f*"), singleton("baz"));

        expected = builder -> builder.startObject().endObject();
        testFilter(expected, sample, emptySet(), singleton("foo"));
        testFilter(expected, sample, singleton("bar"), emptySet());
        testFilter(expected, sample, singleton("f*"), singleton("foo"));
    }

    public void testSingleFieldWithBothExcludesIncludes() throws IOException {
        final Builder expected = builder -> builder
            .startObject()
                .field("pages", 456)
                .field("price", 27.99)
            .endObject();

        testFilter(expected, SAMPLE, singleton("p*"), singleton("properties"));
    }

    public void testObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("tags", "authors");
        Set<String> excludes = singleton("authors.name");

        final Builder expected = builder -> builder
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

        testFilter(expected, SAMPLE, includes, excludes);
    }

    public void testRecursiveObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("**.language", "properties.weight");
        Set<String> excludes = singleton("**.distributors");

        final Builder expected = builder -> builder
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

        testFilter(expected, SAMPLE, includes, excludes);
    }

    public void testRecursiveSameObjectWithBothExcludesIncludes() throws IOException {
        Set<String> includes = singleton("**.distributors");
        Set<String> excludes = singleton("**.distributors");

        final Builder expected = builder -> builder.startObject().endObject();
        testFilter(expected, SAMPLE, includes, excludes);
    }

    public void testRecursiveObjectsPropertiesWithBothExcludesIncludes() throws IOException {
        Set<String> includes = singleton("**.en.*");
        Set<String> excludes = Sets.newHashSet("**.distributors.*.name", "**.street");

        final Builder expected = builder -> builder
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

        testFilter(expected, SAMPLE, includes, excludes);
    }

    public void testWithLfAtEnd() throws IOException {
        final Builder sample = builder -> builder
                .startObject()
                    .startObject("foo")
                        .field("bar", "baz")
                    .endObject()
                .endObject()
                .prettyPrint()
                .lfAtEnd();

        testFilter(sample, sample, singleton("foo"), emptySet());
        testFilter(sample, sample, emptySet(), singleton("bar"));
        testFilter(sample, sample, singleton("f*"), singleton("baz"));

        final Builder expected = builder -> builder.startObject().endObject().prettyPrint().lfAtEnd();
        testFilter(expected, sample, emptySet(), singleton("foo"));
        testFilter(expected, sample, singleton("bar"), emptySet());
        testFilter(expected, sample, singleton("f*"), singleton("foo"));
    }

    public void testBasics() throws Exception {
        final Builder sample = builder -> builder
                .startObject()
                    .field("test1", "value1")
                    .field("test2", "value2")
                    .field("something_else", "value3")
                .endObject();

        Builder expected = builder -> builder
                .startObject()
                    .field("test1", "value1")
                .endObject();
        testFilter(expected, sample, singleton("test1"), emptySet());

        expected = builder -> builder
                .startObject()
                    .field("test1", "value1")
                    .field("test2", "value2")
                .endObject();
        testFilter(expected, sample, singleton("test*"), emptySet());

        expected = builder -> builder
                .startObject()
                    .field("test2", "value2")
                    .field("something_else", "value3")
                .endObject();
        testFilter(expected, sample, emptySet(), singleton("test1"));

        // more complex object...
        final Builder complex = builder -> builder
                .startObject()
                    .startObject("path1")
                        .startArray("path2")
                            .startObject().field("test", "value1").endObject()
                            .startObject().field("test", "value2").endObject()
                        .endArray()
                    .endObject()
                    .field("test1", "value1")
                .endObject();

        expected = builder -> builder
                .startObject()
                    .startObject("path1")
                        .startArray("path2")
                            .startObject().field("test", "value1").endObject()
                            .startObject().field("test", "value2").endObject()
                        .endArray()
                    .endObject()
                .endObject();
        testFilter(expected, complex, singleton("path1"), emptySet());
        testFilter(expected, complex, singleton("path1*"), emptySet());
        testFilter(expected, complex, singleton("path1.path2.*"), emptySet());

        expected = builder -> builder
                .startObject()
                    .field("test1", "value1")
                .endObject();
        testFilter(expected, complex, singleton("test1*"), emptySet());
    }

    /**
     * Generalization of {@link XContentMapValuesTests#testSupplementaryCharactersInPaths()}
     */
    public void testFilterSupplementaryCharactersInPaths() throws IOException {
        final Builder sample = builder -> builder
                .startObject()
                    .field("搜索", 2)
                    .field("指数", 3)
                .endObject();

        Builder expected = builder -> builder
                .startObject()
                    .field("搜索", 2)
                .endObject();
        testFilter(expected, sample, singleton("搜索"), emptySet());

        expected = builder -> builder
                .startObject()
                    .field("指数", 3)
                .endObject();
        testFilter(expected, sample, emptySet(), singleton("搜索"));
    }

    /**
     * Generalization of {@link XContentMapValuesTests#testSharedPrefixes()}
     */
    public void testFilterSharedPrefixes() throws IOException {
        final Builder sample = builder -> builder
                .startObject()
                    .field("foobar", 2)
                    .field("foobaz", 3)
                .endObject();

        Builder expected = builder -> builder
                .startObject()
                    .field("foobar", 2)
                .endObject();
        testFilter(expected, sample, singleton("foobar"), emptySet());

        expected = builder -> builder
                .startObject()
                    .field("foobaz", 3)
                .endObject();
        testFilter(expected, sample, emptySet(), singleton("foobar"));
    }

    /**
     * Generalization of {@link XContentMapValuesTests#testPrefix()}
     */
    public void testFilterPrefix() throws IOException {
        final Builder sample = builder -> builder
                .startObject()
                    .array("photos", "foo", "bar")
                    .field("photosCount", 2)
                .endObject();

        Builder expected = builder -> builder
                .startObject()
                    .field("photosCount", 2)
                .endObject();
        testFilter(expected, sample, singleton("photosCount"), emptySet());
    }

    public void testManyFilters() throws IOException, URISyntaxException {
        Builder deep = builder -> builder.startObject()
            .startObject("system")
            .startObject("process")
            .startObject("cgroup")
            .startObject("memory")
            .startObject("stats")
            .startObject("mapped_file")
            .field("bytes", 100)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        Set<String> manyFilters = Files.readAllLines(
            PathUtils.get(AbstractFilteringTestCase.class.getResource("many_filters.txt").toURI()),
            StandardCharsets.UTF_8
        ).stream().filter(s -> false == s.startsWith("#")).collect(toSet());
        testFilter(deep, deep, manyFilters, emptySet());
    }
}
