/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link XContent} filtering.
 */
public abstract class AbstractFilteringTestCase extends ESTestCase {

    @FunctionalInterface
    protected interface Builder extends CheckedFunction<XContentBuilder, XContentBuilder, IOException> {}

    protected abstract void testFilter(Builder expected, Builder actual, Collection<String> includes, Collection<String> excludes)
        throws IOException;

    /** Sample test case */
    protected static final Builder SAMPLE = builderFor("sample.json");

    protected static Builder builderFor(String file) {
        return builder -> {
            try (InputStream stream = AbstractFilteringTestCase.class.getResourceAsStream(file)) {
                assertThat("Couldn't find [" + file + "]", stream, notNullValue());
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)
                ) {
                    // copyCurrentStructure does not property handle filters when it is passed a json parser. So we hide it.
                    return builder.copyCurrentStructure(new FilterXContentParserWrapper(parser) {
                    });
                }
            }
        };
    }

    public final void testNoFiltering() throws Exception {
        final Builder expected = SAMPLE;

        testFilter(expected, SAMPLE, emptySet(), emptySet());
        testFilter(expected, SAMPLE, singleton("*"), emptySet());
        testFilter(expected, SAMPLE, singleton("**"), emptySet());
        testFilter(expected, SAMPLE, emptySet(), singleton("xyz"));
    }

    public final void testNoMatch() throws Exception {
        final Builder expected = builder -> builder.startObject().endObject();

        testFilter(expected, SAMPLE, singleton("xyz"), emptySet());
        testFilter(expected, SAMPLE, emptySet(), singleton("*"));
        testFilter(expected, SAMPLE, emptySet(), singleton("**"));
    }

    public final void testSimpleFieldInclusive() throws Exception {
        final Builder expected = builder -> builder.startObject().field("title", "My awesome book").endObject();

        testFilter(expected, SAMPLE, singleton("title"), emptySet());
    }

    public final void testSimpleFieldExclusive() throws Exception {
        testFilter(builderFor("sample_no_title.json"), SAMPLE, emptySet(), singleton("title"));
    }

    public final void testSimpleFieldWithWildcardInclusive() throws Exception {
        testFilter(builderFor("sample_just_pr.json"), SAMPLE, singleton("pr*"), emptySet());
    }

    public final void testSimpleFieldWithWildcardExclusive() throws Exception {
        testFilter(builderFor("sample_no_pr.json"), SAMPLE, emptySet(), singleton("pr*"));
    }

    public final void testMultipleFieldsInclusive() throws Exception {
        Builder expected = builder -> builder.startObject().field("title", "My awesome book").field("pages", 456).endObject();
        testFilter(expected, SAMPLE, Sets.newHashSet("title", "pages"), emptySet());
    }

    public final void testMultipleFieldsExclusive() throws Exception {
        testFilter(builderFor("sample_no_title_pages.json"), SAMPLE, emptySet(), Sets.newHashSet("title", "pages"));
    }

    public final void testSimpleArrayInclusive() throws Exception {
        Builder expected = builder -> builder.startObject().startArray("tags").value("elasticsearch").value("java").endArray().endObject();
        testFilter(expected, SAMPLE, singleton("tags"), emptySet());
    }

    public final void testSimpleArrayExclusive() throws Exception {
        testFilter(builderFor("sample_no_tags.json"), SAMPLE, emptySet(), singleton("tags"));
    }

    public final void testSimpleArrayOfObjectsInclusive() throws Exception {
        Builder expected = builderFor("sample_just_authors.json");
        testFilter(expected, SAMPLE, singleton("authors"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.*"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.*name"), emptySet());
    }

    public final void testSimpleArrayOfObjectsExclusive() throws Exception {
        Builder expected = builderFor("sample_no_authors.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("authors"));
        if (removesEmptyArrays()) {
            testFilter(expected, SAMPLE, emptySet(), singleton("authors.*"));
            testFilter(expected, SAMPLE, emptySet(), singleton("authors.*name"));
        }
    }

    protected abstract boolean removesEmptyArrays();

    public void testSimpleArrayOfObjectsPropertyInclusive() throws Exception {
        Builder expected = builderFor("sample_just_authors_lastname.json");
        testFilter(expected, SAMPLE, singleton("authors.lastname"), emptySet());
        testFilter(expected, SAMPLE, singleton("authors.l*"), emptySet());
    }

    public void testSimpleArrayOfObjectsPropertyExclusive() throws Exception {
        Builder expected = builderFor("sample_no_authors_lastname.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("authors.lastname"));
        testFilter(expected, SAMPLE, emptySet(), singleton("authors.l*"));
    }

    public void testRecurseField1Inclusive() throws Exception {
        Builder expected = builderFor("sample_just_names.json");
        testFilter(expected, SAMPLE, singleton("**.name"), emptySet());
    }

    public void testRecurseField1Exclusive() throws Exception {
        Builder expected = builderFor("sample_no_names.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("**.name"));
    }

    public void testRecurseField2Inclusive() throws Exception {
        Builder expected = builderFor("sample_just_properties_names.json");
        testFilter(expected, SAMPLE, singleton("properties.**.name"), emptySet());
    }

    public void testRecurseField2Exclusive() throws Exception {
        Builder expected = builderFor("sample_no_properties_names.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("properties.**.name"));
    }

    public void testRecurseField3Inclusive() throws Exception {
        Builder expected = builderFor("sample_just_properties_en_names.json");
        testFilter(expected, SAMPLE, singleton("properties.*.en.**.name"), emptySet());
    }

    public void testRecurseField3Exclusive() throws Exception {
        Builder expected = builderFor("sample_no_properties_en_names.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("properties.*.en.**.name"));
    }

    public void testRecurseField4Inclusive() throws Exception {
        Builder expected = builderFor("sample_just_properties_distributors_names.json");
        testFilter(expected, SAMPLE, singleton("properties.**.distributors.name"), emptySet());
    }

    public void testRecurseField4Exclusive() throws Exception {
        Builder expected = builderFor("sample_no_properties_distributors_names.json");
        testFilter(expected, SAMPLE, emptySet(), singleton("properties.**.distributors.name"));
    }

    public void testRawField() throws Exception {
        Builder expectedRawField = builder -> {
            builder.startObject();
            builder.field("foo", 0);
            builder.startObject("raw").field("content", "hello world!").endObject();
            return builder.endObject();
        };

        Builder expectedRawFieldRemoved = builder -> {
            builder.startObject();
            builder.field("foo", 0);
            return builder.endObject();
        };

        Builder expectedRawFieldIncluded = builder -> {
            builder.startObject();
            builder.startObject("raw").field("content", "hello world!").endObject();
            return builder.endObject();
        };

        @SuppressWarnings("deprecation")  // Tests filtering with a deprecated method
        Builder sampleWithRaw = builder -> {
            BytesReference raw = BytesReference.bytes(
                XContentBuilder.builder(builder.contentType().xContent()).startObject().field("content", "hello world!").endObject()
            );
            return builder.startObject().field("foo", 0).rawField("raw", raw.streamInput()).endObject();
        };
        testFilter(expectedRawField, sampleWithRaw, emptySet(), emptySet());
        testFilter(expectedRawFieldRemoved, sampleWithRaw, singleton("f*"), emptySet());
        testFilter(expectedRawFieldRemoved, sampleWithRaw, emptySet(), singleton("r*"));
        testFilter(expectedRawFieldIncluded, sampleWithRaw, singleton("r*"), emptySet());
        testFilter(expectedRawFieldIncluded, sampleWithRaw, emptySet(), singleton("f*"));

        sampleWithRaw = builder -> {
            BytesReference raw = BytesReference.bytes(
                XContentBuilder.builder(builder.contentType().xContent()).startObject().field("content", "hello world!").endObject()
            );
            return builder.startObject().field("foo", 0).rawField("raw", raw.streamInput(), builder.contentType()).endObject();
        };
        testFilter(expectedRawField, sampleWithRaw, emptySet(), emptySet());
        testFilter(expectedRawFieldRemoved, sampleWithRaw, singleton("f*"), emptySet());
        testFilter(expectedRawFieldRemoved, sampleWithRaw, emptySet(), singleton("r*"));
        testFilter(expectedRawFieldIncluded, sampleWithRaw, singleton("r*"), emptySet());
        testFilter(expectedRawFieldIncluded, sampleWithRaw, emptySet(), singleton("f*"));
    }

    public void testArrays() throws Exception {
        // Test: Array of values (no filtering)
        Builder sampleArrayOfValues = builder -> {
            builder.startObject();
            builder.startArray("tags").value("lorem").value("ipsum").value("dolor").endArray();
            return builder.endObject();
        };
        testFilter(sampleArrayOfValues, sampleArrayOfValues, singleton("t*"), emptySet());
        testFilter(sampleArrayOfValues, sampleArrayOfValues, singleton("tags"), emptySet());
        testFilter(sampleArrayOfValues, sampleArrayOfValues, emptySet(), singleton("a"));

        // Test: Array of values (with filtering)
        Builder expected = builder -> builder.startObject().endObject();
        testFilter(expected, sampleArrayOfValues, singleton("foo"), emptySet());
        testFilter(expected, sampleArrayOfValues, emptySet(), singleton("t*"));
        testFilter(expected, sampleArrayOfValues, emptySet(), singleton("tags"));

        // Test: Array of objects (no filtering)
        Builder sampleArrayOfObjects = builder -> {
            builder.startObject();
            builder.startArray("tags");
            {
                builder.startObject().field("lastname", "lorem").endObject();
                builder.startObject().field("firstname", "ipsum").endObject();
            }
            builder.endArray();
            return builder.endObject();
        };
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, singleton("t*"), emptySet());
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, singleton("tags"), emptySet());
        testFilter(sampleArrayOfObjects, sampleArrayOfObjects, emptySet(), singleton("a"));

        // Test: Array of objects (with filtering)
        testFilter(expected, sampleArrayOfObjects, singleton("foo"), emptySet());
        testFilter(expected, sampleArrayOfObjects, emptySet(), singleton("t*"));
        testFilter(expected, sampleArrayOfObjects, emptySet(), singleton("tags"));

        // Test: Array of objects (with partial filtering)
        expected = builder -> {
            builder.startObject();
            builder.startArray("tags");
            {
                builder.startObject().field("firstname", "ipsum").endObject();
            }
            builder.endArray();
            return builder.endObject();
        };
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
        Builder expected = builder -> builder.startObject().field("pages", 456).field("price", 27.99).endObject();
        testFilter(expected, SAMPLE, singleton("p*"), singleton("properties"));
    }

    public void testObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("tags", "authors");
        Set<String> excludes = singleton("authors.name");
        testFilter(builderFor("sample_just_tags_authors_no_name.json"), SAMPLE, includes, excludes);
    }

    public void testRecursiveObjectsInArrayWithBothExcludesIncludes() throws IOException {
        Set<String> includes = Sets.newHashSet("**.language", "properties.weight");
        Set<String> excludes = singleton("**.distributors");
        testFilter(builderFor("sample_just_properties_no_distributors.json"), SAMPLE, includes, excludes);
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
        testFilter(builderFor("sample_just_properties_en_no_distributors_name_no_street.json"), SAMPLE, includes, excludes);
    }

    public void testWithLfAtEnd() throws IOException {
        final Builder sample = builder -> {
            builder.startObject();
            builder.startObject("foo").field("bar", "baz").endObject();
            return builder.endObject().prettyPrint().lfAtEnd();
        };

        testFilter(sample, sample, singleton("foo"), emptySet());
        testFilter(sample, sample, emptySet(), singleton("bar"));
        testFilter(sample, sample, singleton("f*"), singleton("baz"));

        final Builder expected = builder -> builder.startObject().endObject().prettyPrint().lfAtEnd();
        testFilter(expected, sample, emptySet(), singleton("foo"));
        testFilter(expected, sample, singleton("bar"), emptySet());
        testFilter(expected, sample, singleton("f*"), singleton("foo"));
    }

    public void testBasics() throws Exception {
        Builder sample = builder -> {
            return builder.startObject().field("test1", "value1").field("test2", "value2").field("something_else", "value3").endObject();
        };

        Builder expected = builder -> builder.startObject().field("test1", "value1").endObject();
        testFilter(expected, sample, singleton("test1"), emptySet());

        expected = builder -> builder.startObject().field("test1", "value1").field("test2", "value2").endObject();
        testFilter(expected, sample, singleton("test*"), emptySet());

        expected = builder -> builder.startObject().field("test2", "value2").field("something_else", "value3").endObject();
        testFilter(expected, sample, emptySet(), singleton("test1"));

        // more complex object...
        Builder complex = builder -> {
            builder.startObject();
            builder.startObject("path1");
            {
                builder.startArray("path2");
                {
                    builder.startObject().field("test", "value1").endObject();
                    builder.startObject().field("test", "value2").endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            builder.field("test1", "value1");
            return builder.endObject();
        };

        expected = builder -> {
            builder.startObject();
            builder.startObject("path1");
            {
                builder.startArray("path2");
                {
                    builder.startObject().field("test", "value1").endObject();
                    builder.startObject().field("test", "value2").endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return builder.endObject();
        };
        testFilter(expected, complex, singleton("path1"), emptySet());
        testFilter(expected, complex, singleton("path1*"), emptySet());
        testFilter(expected, complex, singleton("path1.path2.*"), emptySet());

        expected = builder -> builder.startObject().field("test1", "value1").endObject();
        testFilter(expected, complex, singleton("test1*"), emptySet());
    }

    /**
     * Tests that we can extract paths containing non-ascii characters.
     */
    public void testFilterSupplementaryCharactersInPaths() throws IOException {
        Builder sample = builder -> builder.startObject().field("搜索", 2).field("指数", 3).endObject();

        Builder expected = builder -> builder.startObject().field("搜索", 2).endObject();
        testFilter(expected, sample, singleton("搜索"), emptySet());

        expected = builder -> builder.startObject().field("指数", 3).endObject();
        testFilter(expected, sample, emptySet(), singleton("搜索"));
    }

    /**
     * Tests that we can extract paths which share a prefix with other paths.
     */
    public void testFilterSharedPrefixes() throws IOException {
        Builder sample = builder -> builder.startObject().field("foobar", 2).field("foobaz", 3).endObject();

        Builder expected = builder -> builder.startObject().field("foobar", 2).endObject();
        testFilter(expected, sample, singleton("foobar"), emptySet());

        expected = builder -> builder.startObject().field("foobaz", 3).endObject();
        testFilter(expected, sample, emptySet(), singleton("foobar"));
    }

    /**
     * Tests that we can extract paths which have another path as a prefix.
     */
    public void testFilterPrefix() throws IOException {
        Builder sample = builder -> builder.startObject().array("photos", "foo", "bar").field("photosCount", 2).endObject();
        Builder expected = builder -> builder.startObject().field("photosCount", 2).endObject();
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
        try (InputStream stream = AbstractFilteringTestCase.class.getResourceAsStream("many_filters.txt")) {
            assertThat("Couldn't find [many_filters.txt]", stream, notNullValue());
            Set<String> manyFilters = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)).lines()
                .filter(s -> false == s.startsWith("#"))
                .collect(toSet());
            testFilter(deep, deep, manyFilters, emptySet());
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/80160")
    public void testExcludeWildCardFields() throws IOException {
        Builder sample = builder -> builder.startObject()
            .startObject("include")
            .field("field1", "v1")
            .field("field2", "v2")
            .endObject()
            .field("include2", "vv2")
            .endObject();
        Builder expected = builder -> builder.startObject().startObject("include").field("field1", "v1").endObject().endObject();
        testFilter(expected, sample, singleton("include"), singleton("*.field2"));
    }
}
