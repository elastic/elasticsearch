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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFieldFilter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public class XContentFieldFilterTests extends AbstractFilteringTestCase {
    @Override
    protected void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();
        String[] sourceIncludes;
        if (includes == null) {
            sourceIncludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceIncludes = includes.toArray(new String[includes.size()]);
        }
        String[] sourceExcludes;
        if (excludes == null) {
            sourceExcludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceExcludes = excludes.toArray(new String[excludes.size()]);
        }
        XContentFieldFilter filter = XContentFieldFilter.newFieldFilter(sourceIncludes, sourceExcludes);
        BytesReference ref = filter.apply(toBytesReference(actual, xContentType, humanReadable), xContentType);
        assertMap(XContentHelper.convertToMap(ref, true, xContentType).v2(), matchesMap(toMap(expected, xContentType, humanReadable)));
    }

    public void testPrefixedNamesFilteringTest() throws IOException {
        Builder actual = builder -> builder.startObject().field("obj", "value").field("obj_name", "value_name").endObject();
        Builder expected = builder -> builder.startObject().field("obj_name", "value_name").endObject();
        testFilter(expected, actual, singleton("obj_name"), emptySet());
    }

    public void testNestedFiltering() throws IOException {
        Builder actual = builder -> builder.startObject()
            .field("field", "value")
            .startArray("array")
            .startObject()
            .field("nested", 2)
            .field("nested_2", 3)
            .endObject()
            .endArray()
            .endObject();
        Builder expected = builder -> builder.startObject()
            .startArray("array")
            .startObject()
            .field("nested", 2)
            .endObject()
            .endArray()
            .endObject();
        testFilter(expected, actual, singleton("array.nested"), emptySet());

        expected = builder -> builder.startObject()
            .startArray("array")
            .startObject()
            .field("nested", 2)
            .field("nested_2", 3)
            .endObject()
            .endArray()
            .endObject();
        testFilter(expected, actual, singleton("array.*"), emptySet());

        actual = builder -> builder.startObject()
            .field("field", "value")
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .endObject();

        expected = builder -> builder.startObject().startObject("obj").field("field", "value").endObject().endObject();
        testFilter(expected, actual, singleton("obj.field"), emptySet());

        expected = builder -> builder.startObject()
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .endObject();
        testFilter(expected, actual, singleton("obj.*"), emptySet());
    }

    public void testCompleteObjectFiltering() throws IOException {

        Builder actual = builder -> builder.startObject()
            .field("field", "value")
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .startArray("array")
            .startObject()
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .endArray()
            .endObject();
        Builder expected = builder -> builder.startObject()
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .endObject();
        testFilter(expected, actual, singleton("obj"), emptySet());

        expected = builder -> builder.startObject().startObject("obj").field("field", "value").endObject().endObject();
        testFilter(expected, actual, singleton("obj"), singleton("*.field2"));

        expected = builder -> builder.startObject()
            .startArray("array")
            .startObject()
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .endArray()
            .endObject();
        testFilter(expected, actual, singleton("array"), emptySet());

        expected = builder -> builder.startObject()
            .startArray("array")
            .startObject()
            .field("field", "value")
            .endObject()
            .endArray()
            .endObject();
        testFilter(expected, actual, singleton("array"), singleton("*.field2"));
    }

    public void testFilterIncludesUsingStarPrefix() throws IOException {
        Builder actual = builder -> builder.startObject()
            .field("field", "value")
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .startObject("n_obj")
            .field("n_field", "value")
            .field("n_field2", "value2")
            .endObject()
            .endObject();
        Builder expected = builder -> builder.startObject().startObject("obj").field("field2", "value2").endObject().endObject();
        testFilter(expected, actual, singleton("*.field2"), emptySet());

        // only objects
        expected = builder -> builder.startObject()
            .startObject("obj")
            .field("field", "value")
            .field("field2", "value2")
            .endObject()
            .startObject("n_obj")
            .field("n_field", "value")
            .field("n_field2", "value2")
            .endObject()
            .endObject();
        testFilter(expected, actual, singleton("*.*"), emptySet());

        expected = builder -> builder.startObject()
            .field("field", "value")
            .startObject("obj")
            .field("field", "value")
            .endObject()
            .startObject("n_obj")
            .field("n_field", "value")
            .endObject()
            .endObject();
        testFilter(expected, actual, singleton("*"), singleton("*.*2"));
    }

    public void testFilterWithEmptyIncludesExcludes() throws IOException {
        Builder actual = builder -> builder.startObject().field("field", "value").endObject();
        testFilter(actual, actual, emptySet(), emptySet());
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingIncludes() throws IOException {
        Builder actual = builder -> builder.startObject().startObject("obj").endObject().endObject();
        testFilter(actual, actual, singleton("obj"), emptySet());
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingExcludes() throws IOException {
        Builder actual = builder -> builder.startObject().startObject("obj").endObject().endObject();
        testFilter(actual, actual, emptySet(), singleton("nonExistingField"));
    }

    @AwaitsFix(bugUrl = "https://github.com/FasterXML/jackson-core/pull/729")
    public void testNotOmittingObjectsWithExcludedProperties() throws IOException {
        Builder actual = builder -> builder.startObject().startObject("obj").field("f1", "f2").endObject().endObject();
        Builder expected = builder -> builder.startObject().startObject("obj").endObject().endObject();
        testFilter(expected, actual, emptySet(), singleton("obj.f1"));
    }

    public void testNotOmittingObjectWithNestedExcludedObject() throws IOException {
        Builder actual = builder -> builder.startObject()
            .startObject("obj1")
            .startObject("obj2")
            .startObject("obj3")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        Builder expected = builder -> builder.startObject().startObject("obj1").endObject().endObject();
        // implicit include
        testFilter(expected, actual, emptySet(), singleton("*.obj2"));

        // explicit include
        testFilter(expected, actual, singleton("obj1"), singleton("*.obj2"));

        // wildcard include
        expected = builder -> builder.startObject().startObject("obj1").startObject("obj2").endObject().endObject().endObject();
        testFilter(expected, actual, singleton("*.obj2"), singleton("*.obj3"));
    }

    public void testIncludingObjectWithNestedIncludedObject() throws IOException {
        Builder actual = builder -> builder.startObject().startObject("obj1").startObject("obj2").endObject().endObject().endObject();
        testFilter(actual, actual, singleton("*.obj2"), emptySet());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/83178")
    public void testDotsInFieldNames() throws IOException {
        Builder actual = builder -> builder.startObject()
            .field("foo.bar", 2)
            .startObject("foo")
            .field("baz", 3)
            .endObject()
            .field("quux", 5)
            .endObject();
        Builder expected = builder -> builder.startObject().field("foo.bar", 2).startObject("foo").field("baz", 3).endObject().endObject();
        testFilter(expected, actual, singleton("foo"), emptySet());

        // dots in field names in excludes
        expected = builder -> builder.startObject().field("quux", 5).endObject();
        testFilter(expected, actual, emptySet(), singleton("foo"));
    }

    /**
    * Tests that we can extract paths containing non-ascii characters.
    * See {@link AbstractFilteringTestCase#testFilterSupplementaryCharactersInPaths()}
    * for a similar test but for XContent.
    */
    public void testSupplementaryCharactersInPaths() throws IOException {
        Builder actual = builder -> builder.startObject().field("搜索", 2).field("指数", 3).endObject();
        Builder expected = builder -> builder.startObject().field("搜索", 2).endObject();
        testFilter(expected, actual, singleton("搜索"), emptySet());
        expected = builder -> builder.startObject().field("指数", 3).endObject();
        testFilter(expected, actual, emptySet(), singleton("搜索"));
    }

    /**
    * Tests that we can extract paths which share a prefix with other paths.
    * See {@link AbstractFilteringTestCase#testFilterSharedPrefixes()}
    * for a similar test but for XContent.
    */
    public void testSharedPrefixes() throws IOException {
        Builder actual = builder -> builder.startObject().field("foobar", 2).field("foobaz", 3).endObject();
        Builder expected = builder -> builder.startObject().field("foobar", 2).endObject();
        testFilter(expected, actual, singleton("foobar"), emptySet());

        expected = builder -> builder.startObject().field("foobaz", 3).endObject();
        testFilter(expected, actual, emptySet(), singleton("foobar"));
    }

    @AwaitsFix(bugUrl = "https://github.com/FasterXML/jackson-core/pull/729")
    public void testArraySubFieldExclusion() throws IOException {
        Builder actual = builder -> builder.startObject()
            .field("field", "value")
            .startArray("array")
            .startObject()
            .field("exclude", "bar")
            .endObject()
            .endArray()
            .endObject();
        Builder expected = builder -> builder.startObject().field("field", "value").startArray("array").endArray().endObject();
        testFilter(expected, actual, emptySet(), singleton("array.exclude"));
    }

    @AwaitsFix(bugUrl = "https://github.com/FasterXML/jackson-core/pull/729")
    public void testEmptyArraySubFieldsExclusion() throws IOException {
        Builder actual = builder -> builder.startObject().field("field", "value").startArray("array").endArray().endObject();
        testFilter(actual, actual, emptySet(), singleton("array.exclude"));
    }

    public void testEmptyArraySubFieldsInclusion() throws IOException {
        Builder actual = builder -> builder.startObject().field("field", "value").startArray("array").endArray().endObject();
        Builder expected = builder -> builder.startObject().endObject();
        testFilter(expected, actual, singleton("array.include"), emptySet());

        expected = builder -> builder.startObject().startArray("array").endArray().endObject();
        testFilter(expected, actual, Set.of("array", "array.include"), emptySet());
    }

    public void testEmptyObjectsSubFieldsInclusion() throws IOException {
        Builder actual = builder -> builder.startObject().field("field", "value").startObject("object").endObject().endObject();
        Builder expected = builder -> builder.startObject().endObject();
        testFilter(expected, actual, singleton("object.include"), emptySet());

        expected = builder -> builder.startObject().startObject("object").endObject().endObject();
        testFilter(expected, actual, Set.of("object", "object.include"), emptySet());
    }

    /**
    * Tests that we can extract paths which have another path as a prefix.
    * See {@link AbstractFilteringTestCase#testFilterPrefix()}
    * for a similar test but for XContent.
    */
    public void testPrefix() throws IOException {
        Builder actual = builder -> builder.startObject().array("photos", "foo", "bar").field("photosCount", 2).endObject();
        Builder expected = builder -> builder.startObject().field("photosCount", 2).endObject();
        testFilter(expected, actual, singleton("photosCount"), emptySet());
    }

    public void testEmptySource() throws IOException {
        final CheckedFunction<XContentType, BytesReference, IOException> emptyValueSupplier = xContentType -> {
            BytesStreamOutput bStream = new BytesStreamOutput();
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType, bStream).map(Collections.emptyMap());
            builder.close();
            return bStream.bytes();
        };
        final XContentType xContentType = randomFrom(XContentType.values());
        // null value for parser filter
        assertEquals(
            emptyValueSupplier.apply(xContentType),
            XContentFieldFilter.newFieldFilter(new String[0], new String[0]).apply(null, xContentType)
        );
        // empty bytes for parser filter
        assertEquals(
            emptyValueSupplier.apply(xContentType),
            XContentFieldFilter.newFieldFilter(new String[0], new String[0]).apply(BytesArray.EMPTY, xContentType)
        );
        // null value for map filter
        assertEquals(
            emptyValueSupplier.apply(xContentType),
            XContentFieldFilter.newFieldFilter(new String[0], new String[] { "test*" }).apply(null, xContentType)
        );
        // empty bytes for map filter
        assertEquals(
            emptyValueSupplier.apply(xContentType),
            XContentFieldFilter.newFieldFilter(new String[0], new String[] { "test*" }).apply(BytesArray.EMPTY, xContentType)
        );
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
