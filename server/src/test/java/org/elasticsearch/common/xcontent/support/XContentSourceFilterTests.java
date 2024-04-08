/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public class XContentSourceFilterTests extends AbstractFilteringTestCase {
    @Override
    protected void testFilter(Builder expected, Builder actual, Collection<String> includes, Collection<String> excludes)
        throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();
        String[] sourceIncludes;
        if (includes == null) {
            sourceIncludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceIncludes = includes.toArray(String[]::new);
        }
        String[] sourceExcludes;
        if (excludes == null) {
            sourceExcludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceExcludes = excludes.toArray(String[]::new);
        }
        SourceFilter filter = new SourceFilter(sourceIncludes, sourceExcludes);
        Source filtered = Source.fromBytes(toBytesReference(actual, xContentType, humanReadable), xContentType).filter(filter);
        assertMap(
            XContentHelper.convertToMap(filtered.internalSourceRef(), true, xContentType).v2(),
            matchesMap(toMap(expected, xContentType, humanReadable))
        );
    }

    private void testFilter(String expectedJson, String actualJson, Collection<String> includes, Collection<String> excludes)
        throws IOException {
        CheckedFunction<String, Builder, IOException> toBuilder = json -> {
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(json), XContentType.JSON);
            if ((parser.currentToken() == null) && (parser.nextToken() == null)) {
                return builder -> builder;
            }
            return builder -> builder.copyCurrentStructure(parser);
        };
        testFilter(toBuilder.apply(expectedJson), toBuilder.apply(actualJson), includes, excludes);
    }

    public void testPrefixedNamesFilteringTest() throws IOException {
        String actual = """
            {
                "obj": "value",
                "obj_name": "value_name"
            }
            """;
        String expected = """
            {
                "obj_name": "value_name"
            }
            """;
        testFilter(expected, actual, singleton("obj_name"), emptySet());
    }

    public void testDuplicatedIncludes() throws IOException {
        String actual = """
            {
                "obj": "value",
                "obj_name": "value_name"
            }
            """;
        String expected = """
            {
                "obj_name": "value_name"
            }
            """;
        testFilter(expected, actual, List.of("obj_name", "obj_name"), emptySet());
    }

    public void testDuplicatedExcludes() throws IOException {
        String actual = """
            {
                "obj": "value",
                "obj_name": "value_name"
            }
            """;
        String expected = """
            {
                "obj": "value"
            }
            """;
        testFilter(expected, actual, emptySet(), List.of("obj_name", "obj_name"));
    }

    public void testNestedFiltering() throws IOException {
        String actual = """
            {
                "field": "value",
                "array": [{
                    "nested": 2,
                    "nested_2": 3
                }]
            }
            """;
        String expected = """
            {
                "array": [{
                    "nested": 2
                }]
            }
            """;
        testFilter(expected, actual, singleton("array.nested"), emptySet());

        expected = """
            {
                "array": [{
                    "nested": 2,
                    "nested_2": 3
                }]
            }
            """;
        testFilter(expected, actual, singleton("array.*"), emptySet());

        actual = """
            {
                "field": "value",
                "obj": {
                    "field": "value",
                    "field2": "value2"
                }
            }
            """;

        expected = """
            {
                "obj": {
                    "field": "value"
                }
            }
            """;
        testFilter(expected, actual, singleton("obj.field"), emptySet());

        expected = """
            {
                "obj": {
                    "field": "value",
                    "field2": "value2"
                }
            }
            """;
        testFilter(expected, actual, singleton("obj.*"), emptySet());
    }

    public void testCompleteObjectFiltering() throws IOException {
        String actual = """
            {
                "field": "value",
                "obj": {
                    "field": "value",
                    "field2": "value2"
                },
                "array": [{
                    "field": "value",
                    "field2": "value2"
                }]
            }
            """;
        String expected = """
            {
                "obj": {
                    "field": "value",
                    "field2": "value2"
                }
            }
            """;
        testFilter(expected, actual, singleton("obj"), emptySet());

        expected = """
            {
                "obj": {
                    "field": "value"
                }
            }
            """;
        testFilter(expected, actual, singleton("obj"), singleton("*.field2"));

        expected = """
            {
                "array": [{
                    "field": "value",
                    "field2": "value2"
                }]
            }
            """;
        testFilter(expected, actual, singleton("array"), emptySet());

        expected = """
            {
                "array": [{
                    "field": "value"
                }]
            }
            """;
        testFilter(expected, actual, singleton("array"), singleton("*.field2"));
    }

    public void testFilterIncludesUsingStarPrefix() throws IOException {
        String actual = """
            {
                "field": "value",
                "obj": {
                    "field": "value",
                    "field2": "value2"
                },
                "n_obj": {
                    "n_field": "value",
                    "n_field2": "value2"
                }
            }
            """;
        String expected = """
            {
                "obj": {
                    "field2": "value2"
                }
            }
            """;
        testFilter(expected, actual, singleton("*.field2"), emptySet());

        // only objects
        expected = """
            {
                "obj": {
                    "field": "value",
                    "field2": "value2"
                },
                "n_obj": {
                    "n_field": "value",
                    "n_field2": "value2"
                }
            }
            """;
        testFilter(expected, actual, singleton("*.*"), emptySet());

        expected = """
            {
                "field": "value",
                "obj": {
                    "field": "value"
                },
                "n_obj": {
                    "n_field": "value"
                }
            }
            """;
        testFilter(expected, actual, singleton("*"), singleton("*.*2"));
    }

    public void testFilterWithEmptyIncludesExcludes() throws IOException {
        String actual = """
            {
                "field": "value"
            }
            """;
        testFilter(actual, actual, emptySet(), emptySet());
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingIncludes() throws IOException {
        String actual = """
            {
                "obj": {}
            }
            """;
        testFilter(actual, actual, singleton("obj"), emptySet());
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingExcludes() throws IOException {
        String actual = """
            {
                "obj": {}
            }
            """;
        testFilter(actual, actual, emptySet(), singleton("nonExistingField"));
    }

    // wait for PR https://github.com/FasterXML/jackson-core/pull/729 to be introduced
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/80160")
    public void testNotOmittingObjectsWithExcludedProperties() throws IOException {
        String actual = """
            {
                "obj": {
                    "f1": "f2"
                }
            }
            """;
        String expected = """
            {
                "obj": {}
            }
            """;
        testFilter(expected, actual, emptySet(), singleton("obj.f1"));
    }

    public void testNotOmittingObjectWithNestedExcludedObject() throws IOException {
        String actual = """
            {
                "obj1": {
                    "obj2": {
                        "obj3": {}
                    }
                }
            }
            """;
        String expected = """
            {
                "obj1": {}
            }
            """;
        // implicit include
        testFilter(expected, actual, emptySet(), singleton("*.obj2"));

        // explicit include
        testFilter(expected, actual, singleton("obj1"), singleton("*.obj2"));

        // wildcard include
        expected = """
            {
                "obj1": {
                    "obj2": {}
                }
            }
            """;
        testFilter(expected, actual, singleton("*.obj2"), singleton("*.obj3"));
    }

    public void testIncludingObjectWithNestedIncludedObject() throws IOException {
        String actual = """
            {
                "obj1": {
                    "obj2": {}
                }
            }
            """;
        testFilter(actual, actual, singleton("*.obj2"), emptySet());
    }

    public void testDotsInFieldNames() throws IOException {
        String actual = """
            {
                "foo.bar": 2,
                "foo": {
                    "baz": 3
                },
                "quux": 5
            }
            """;
        String expected = """
            {
                "foo.bar": 2,
                "foo": {
                    "baz": 3
                }
            }
            """;
        testFilter(expected, actual, singleton("foo"), emptySet());

        // dots in field names in excludes
        expected = """
            {
                "quux": 5
            }
            """;
        testFilter(expected, actual, emptySet(), singleton("foo"));
    }

    /**
    * Tests that we can extract paths containing non-ascii characters.
    * See {@link AbstractFilteringTestCase#testFilterSupplementaryCharactersInPaths()}
    * for a similar test but for XContent.
    */
    public void testSupplementaryCharactersInPaths() throws IOException {
        String actual = """
            {
                "搜索": 2,
                "指数": 3
            }
            """;
        String expected = """
            {
                "搜索": 2
            }
            """;
        testFilter(expected, actual, singleton("搜索"), emptySet());
        expected = """
            {
                "指数": 3
            }
            """;
        testFilter(expected, actual, emptySet(), singleton("搜索"));
    }

    /**
    * Tests that we can extract paths which share a prefix with other paths.
    * See {@link AbstractFilteringTestCase#testFilterSharedPrefixes()}
    * for a similar test but for XContent.
    */
    public void testSharedPrefixes() throws IOException {
        String actual = """
            {
                "foobar": 2,
                "foobaz": 3
            }
            """;
        String expected = """
            {
                "foobar": 2
            }
            """;
        testFilter(expected, actual, singleton("foobar"), emptySet());
        expected = """
            {
                "foobaz": 3
            }
            """;
        testFilter(expected, actual, emptySet(), singleton("foobar"));
    }

    // wait for PR https://github.com/FasterXML/jackson-core/pull/729 to be introduced
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/80160")
    public void testArraySubFieldExclusion() throws IOException {
        String actual = """
            {
                "field": "value",
                "array": [{
                    "exclude": "bar"
                }]
            }
            """;
        String expected = """
            {
                "field": "value",
                "array": []
            }
            """;
        testFilter(expected, actual, emptySet(), singleton("array.exclude"));
    }

    // wait for PR https://github.com/FasterXML/jackson-core/pull/729 to be introduced
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/80160")
    public void testEmptyArraySubFieldsExclusion() throws IOException {
        String actual = """
            {
                "field": "value",
                "array": []
            }
            """;
        testFilter(actual, actual, emptySet(), singleton("array.exclude"));
    }

    public void testEmptyArraySubFieldsInclusion() throws IOException {
        String actual = """
            {
                "field": "value",
                "array": []
            }
            """;
        String expected = "{}";
        testFilter(expected, actual, singleton("array.include"), emptySet());

        expected = """
            {
                "array": []
            }
            """;
        testFilter(expected, actual, Set.of("array", "array.include"), emptySet());
    }

    public void testEmptyObjectsSubFieldsInclusion() throws IOException {
        String actual = """
            {
                "field": "value",
                "object": {}
            }
            """;
        String expected = "{}";
        testFilter(expected, actual, singleton("object.include"), emptySet());

        expected = """
            {
                "object": {}
            }
            """;
        testFilter(expected, actual, Set.of("object", "object.include"), emptySet());
    }

    /**
    * Tests that we can extract paths which have another path as a prefix.
    * See {@link AbstractFilteringTestCase#testFilterPrefix()}
    * for a similar test but for XContent.
    */
    public void testPrefix() throws IOException {
        String actual = """
            {
                "photos": ["foo", "bar"],
                "photosCount": 2
            }
            """;
        String expected = """
            {
                "photosCount": 2
            }
            """;
        testFilter(expected, actual, singleton("photosCount"), emptySet());
    }

    public void testEmptySource() throws IOException {
        SourceFilter empty = new SourceFilter(new String[0], new String[0]);
        SourceFilter excludeWildcard = new SourceFilter(new String[0], new String[] { "test* " });
        for (XContentType xContentType : XContentType.values()) {
            assertEquals(Map.of(), Source.fromBytes(null, xContentType).filter(empty).source());
            assertEquals(Map.of(), Source.fromBytes(null, xContentType).filter(excludeWildcard).source());
            assertEquals(Map.of(), Source.fromBytes(BytesArray.EMPTY, xContentType).filter(empty).source());
            assertEquals(Map.of(), Source.fromBytes(BytesArray.EMPTY, xContentType).filter(excludeWildcard).source());
            assertEquals(Map.of(), Source.fromMap(null, xContentType).filter(empty).source());
            assertEquals(Map.of(), Source.fromMap(Map.of(), xContentType).filter(excludeWildcard).source());
        }
    }

    private BytesReference toBytesReference(Builder builder, XContentType xContentType, boolean humanReadable) throws IOException {
        return toXContent((ToXContentObject) (xContentBuilder, params) -> builder.apply(xContentBuilder), xContentType, humanReadable);
    }

    private static Map<String, Object> toMap(Builder test, XContentType xContentType, boolean humanReadable) throws IOException {
        ToXContentObject toXContent = (builder, params) -> test.apply(builder);
        return convertToMap(toXContent(toXContent, xContentType, humanReadable), true, xContentType).v2();
    }

    @Override
    protected boolean removesEmptyArrays() {
        return false;
    }
}
