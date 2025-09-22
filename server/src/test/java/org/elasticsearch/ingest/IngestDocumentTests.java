/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.CLASSIC;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.FLEXIBLE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestDocumentTests extends ESTestCase {

    private static final ZonedDateTime BOGUS_TIMESTAMP = ZonedDateTime.of(2016, 10, 23, 0, 0, 0, 0, ZoneOffset.UTC);
    private IngestDocument document;
    private static final String DOUBLE_ARRAY_FIELD = "double_array_field";
    private static final String DOUBLE_DOUBLE_ARRAY_FIELD = "double_double_array";

    @Before
    public void setTestIngestDocument() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> ingestMap = new HashMap<>();
        ingestMap.put("timestamp", BOGUS_TIMESTAMP);
        document.put("_ingest", ingestMap);
        document.put("foo", "bar");
        document.put("int", 123);
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("buzz", "hello world");
        innerObject.put("foo_null", null);
        innerObject.put("1", "bar");
        List<String> innerInnerList = new ArrayList<>();
        innerInnerList.add("item1");
        List<Object> innerList = new ArrayList<>();
        innerList.add(innerInnerList);
        innerObject.put("list", innerList);
        document.put("fizz", innerObject);
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> value = new HashMap<>();
        value.put("field", "value");
        list.add(value);
        list.add(null);
        document.put("list", list);

        List<String> list2 = new ArrayList<>();
        list2.add("foo");
        list2.add("bar");
        list2.add("baz");
        document.put("list2", list2);
        document.put(DOUBLE_ARRAY_FIELD, DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray());
        document.put(
            DOUBLE_DOUBLE_ARRAY_FIELD,
            new double[][] {
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray(),
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray(),
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray() }
        );

        var dots = new HashMap<>(
            Map.of(
                "foo.bar.baz",
                "fizzbuzz",
                "dotted.integers",
                new HashMap<>(Map.of("a", 1, "b.c", 2, "d.e.f", 3, "g.h.i.j", 4, "k.l.m.n.o", 5)),
                "inaccessible",
                new HashMap<>(Map.of("a", new HashMap<>(Map.of("b", new HashMap<>(Map.of("c", "visible")))), "a.b.c", "inaccessible")),
                "arrays",
                new HashMap<>(
                    Map.of(
                        "dotted.strings",
                        new ArrayList<>(List.of("a", "b", "c", "d")),
                        "dotted.objects",
                        new ArrayList<>(List.of(new HashMap<>(Map.of("foo", "bar")), new HashMap<>(Map.of("baz", "qux")))),
                        "dotted.other",
                        new ArrayList<>() {
                            {
                                add(null);
                                add("");
                            }
                        }
                    )
                ),
                "single_fieldname",
                new HashMap<>(
                    Map.of(
                        "multiple.fieldnames",
                        new HashMap<>(Map.of("single_fieldname_again", new HashMap<>(Map.of("multiple.fieldnames.again", "result"))))
                    )
                )
            )
        );
        dots.put("foo.bar.null", null);
        document.put("dots", dots);
        document.put("dotted.bar.baz", true);
        document.put("dotted.foo.bar.baz", new HashMap<>(Map.of("qux.quux", true)));
        document.put("dotted.bar.baz_null", null);

        this.document = new IngestDocument("index", "id", 1, null, null, document);
    }

    /**
     * Executes an action against an ingest document using the provided access pattern. A synthetic pipeline instance with the provided
     * access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param accessPattern The access pattern to use when executing the block of code
     * @param action A consumer which takes the updated ingest document and performs an action with it
     * @throws Exception Any exception thrown from the provided consumer
     */
    private void doWithAccessPattern(IngestPipelineFieldAccessPattern accessPattern, Consumer<IngestDocument> action) throws Exception {
        IngestPipelineTestUtils.doWithAccessPattern(accessPattern, document, action);
    }

    /**
     * Executes an action against an ingest document using a randomly selected access pattern. A synthetic pipeline instance with the
     * selected access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param action A consumer which takes the updated ingest document and performs an action with it
     * @throws Exception Any exception thrown from the provided consumer
     */
    private void doWithRandomAccessPattern(Consumer<IngestDocument> action) throws Exception {
        IngestPipelineTestUtils.doWithRandomAccessPattern(document, action);
    }

    private void assertPathValid(IngestDocument doc, String path) {
        // The fields being checked do not exist, so they all return false when running hasField
        assertFalse(doc.hasField(path));
    }

    private void assertPathInvalid(IngestDocument doc, String path, String errorMessage) {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> doc.hasField(path));
        assertThat(expected.getMessage(), equalTo(errorMessage));
    }

    public void testPathParsingLogic() throws Exception {
        // Force a blank document for this test
        document = new IngestDocument("index", "id", 1, null, null, new HashMap<>());

        doWithRandomAccessPattern((doc) -> {
            assertPathInvalid(doc, null, "path cannot be null nor empty");
            assertPathInvalid(doc, "", "path cannot be null nor empty");
            assertPathValid(doc, "a");
            assertPathValid(doc, "ab");
            assertPathValid(doc, "abc");
            assertPathValid(doc, "a.b");
            assertPathValid(doc, "a.b.c");
            // Trailing empty strings are trimmed by field path parsing logic
            assertPathValid(doc, "a.");
            assertPathValid(doc, "a..");
            assertPathValid(doc, "a...");
            // Empty field names are not allowed in the beginning or middle of the path though
            assertPathInvalid(doc, ".a.b", "path [.a.b] is not valid");
            assertPathInvalid(doc, "a..b", "path [a..b] is not valid");
        });

        doWithAccessPattern(CLASSIC, (doc) -> {
            // Classic allows number fields because they are treated as either field names or array indices depending on context
            assertPathValid(doc, "a.0");
            // Classic allows square brackets because it is not part of it's syntax
            assertPathValid(doc, "a[0]");
            assertPathValid(doc, "a[]");
            assertPathValid(doc, "a][");
            assertPathValid(doc, "[");
            assertPathValid(doc, "a[");
            assertPathValid(doc, "[a");
            assertPathValid(doc, "]");
            assertPathValid(doc, "a]");
            assertPathValid(doc, "]a");
            assertPathValid(doc, "[]");
            assertPathValid(doc, "][");
            assertPathValid(doc, "[a]");
            assertPathValid(doc, "]a[");
            assertPathValid(doc, "[]a");
            assertPathValid(doc, "][a");
        });

        doWithAccessPattern(FLEXIBLE, (doc) -> {
            // Flexible has specific handling of square brackets
            assertPathInvalid(doc, "a[0]", "path [a[0]] is not valid");
            assertPathInvalid(doc, "a[]", "path [a[]] is not valid");
            assertPathInvalid(doc, "a][", "path [a][] is not valid");
            assertPathInvalid(doc, "[", "path [[] is not valid");
            assertPathInvalid(doc, "a[", "path [a[] is not valid");
            assertPathInvalid(doc, "[a", "path [[a] is not valid");
            assertPathInvalid(doc, "]", "path []] is not valid");
            assertPathInvalid(doc, "a]", "path [a]] is not valid");
            assertPathInvalid(doc, "]a", "path []a] is not valid");
            assertPathInvalid(doc, "[]", "path [[]] is not valid");
            assertPathInvalid(doc, "][", "path [][] is not valid");
            assertPathInvalid(doc, "[a]", "path [[a]] is not valid");
            assertPathInvalid(doc, "]a[", "path []a[] is not valid");
            assertPathInvalid(doc, "[]a", "path [[]a] is not valid");
            assertPathInvalid(doc, "][a", "path [][a] is not valid");

            assertPathInvalid(doc, "a[0].b", "path [a[0].b] is not valid");
            assertPathInvalid(doc, "a[0].b[1]", "path [a[0].b[1]] is not valid");
            assertPathInvalid(doc, "a[0].b[1].c", "path [a[0].b[1].c] is not valid");
            assertPathInvalid(doc, "a[0].b[1].c[2]", "path [a[0].b[1].c[2]] is not valid");
            assertPathInvalid(doc, "a[0][1].c[2]", "path [a[0][1].c[2]] is not valid");
            assertPathInvalid(doc, "a[0].b[1][2]", "path [a[0].b[1][2]] is not valid");
            assertPathInvalid(doc, "a[0][1][2]", "path [a[0][1][2]] is not valid");

            assertPathInvalid(doc, "a[0][", "path [a[0][] is not valid");
            assertPathInvalid(doc, "a[0]]", "path [a[0]]] is not valid");
            assertPathInvalid(doc, "a[0]blahblah", "path [a[0]blahblah] is not valid");
        });
    }

    public void testSimpleGetFieldValue() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            assertThat(doc.getFieldValue("foo", String.class), equalTo("bar"));
            assertThat(doc.getFieldValue("int", Integer.class), equalTo(123));
            assertThat(doc.getFieldValue("_source.foo", String.class), equalTo("bar"));
            assertThat(doc.getFieldValue("_source.int", Integer.class), equalTo(123));
            assertThat(doc.getFieldValue("_index", String.class), equalTo("index"));
            assertThat(doc.getFieldValue("_id", String.class), equalTo("id"));
            assertThat(
                doc.getFieldValue("_ingest.timestamp", ZonedDateTime.class),
                both(notNullValue()).and(not(equalTo(BOGUS_TIMESTAMP)))
            );
            assertThat(doc.getFieldValue("_source._ingest.timestamp", ZonedDateTime.class), equalTo(BOGUS_TIMESTAMP));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            assertThat(doc.getFieldValue("dots.foo.bar.baz", String.class), equalTo("fizzbuzz"));
            assertThat(doc.getFieldValue("dotted.bar.baz", Boolean.class), equalTo(true));
        });
    }

    public void testGetFieldValueIgnoreMissing() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            assertThat(doc.getFieldValue("foo", String.class, randomBoolean()), equalTo("bar"));
            assertThat(doc.getFieldValue("int", Integer.class, randomBoolean()), equalTo(123));

            // if ignoreMissing is true, we just return nulls for values that aren't found
            assertThat(doc.getFieldValue("nonsense", Integer.class, true), nullValue());
            assertThat(doc.getFieldValue("some.nonsense", Integer.class, true), nullValue());
            assertThat(doc.getFieldValue("fizz.some.nonsense", Integer.class, true), nullValue());
        });
        doWithAccessPattern(CLASSIC, (doc) -> {
            // if ignoreMissing is false, we throw an exception for values that aren't found
            IllegalArgumentException e;
            e = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("fizz.some.nonsense", Integer.class, false));
            assertThat(e.getMessage(), is("field [some] not present as part of path [fizz.some.nonsense]"));

            // if ignoreMissing is true, and the object is present-and-of-the-wrong-type, then we also throw an exception
            e = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("int", Boolean.class, true));
            assertThat(e.getMessage(), is("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.Boolean]"));
        });
        doWithAccessPattern(FLEXIBLE, (doc -> {
            // if ignoreMissing is false, we throw an exception for values that aren't found
            IllegalArgumentException e;
            e = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("fizz.some.nonsense", Integer.class, false));
            assertThat(e.getMessage(), is("field [some.nonsense] not present as part of path [fizz.some.nonsense]"));

            // if ignoreMissing is true, and the object is present-and-of-the-wrong-type, then we also throw an exception
            e = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("int", Boolean.class, true));
            assertThat(e.getMessage(), is("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.Boolean]"));
        }));
    }

    public void testGetSourceObject() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("_source", Object.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testGetIngestObject() throws Exception {
        doWithRandomAccessPattern((doc) -> assertThat(doc.getFieldValue("_ingest", Map.class), notNullValue()));
    }

    public void testGetEmptyPathAfterStrippingOutPrefix() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("_source.", Object.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("_ingest.", Object.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testGetFieldValueNullValue() throws Exception {
        doWithRandomAccessPattern((doc) -> assertThat(doc.getFieldValue("fizz.foo_null", Object.class), nullValue()));
    }

    public void testSimpleGetFieldValueTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("int", String.class));
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("foo", Integer.class));
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testSimpleGetFieldValueIgnoreMissingAndTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("int", String.class, randomBoolean()));
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("foo", Integer.class, randomBoolean()));
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testNestedGetFieldValue() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            assertThat(doc.getFieldValue("fizz.buzz", String.class), equalTo("hello world"));
            assertThat(doc.getFieldValue("fizz.1", String.class), equalTo("bar"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            // Several layers of dotted field names dots -> dotted.integers -> [a - k.l.m.n.o]
            assertThat(doc.getFieldValue("dots.dotted.integers.a", Integer.class), equalTo(1));
            assertThat(doc.getFieldValue("dots.dotted.integers.b.c", Integer.class), equalTo(2));
            assertThat(doc.getFieldValue("dots.dotted.integers.d.e.f", Integer.class), equalTo(3));
            assertThat(doc.getFieldValue("dots.dotted.integers.g.h.i.j", Integer.class), equalTo(4));
            assertThat(doc.getFieldValue("dots.dotted.integers.k.l.m.n.o", Integer.class), equalTo(5));

            // The dotted field {dots: {inaccessible: {a.b.c: "inaccessible"}}} is inaccessible because
            // the field {dots: {inaccessible: {a: {b: {c: "visible"}}}}} exists
            assertThat(doc.getFieldValue("dots.inaccessible.a.b.c", String.class), equalTo("visible"));

            // Mixing multiple single tokens with dotted tokens
            assertThat(
                doc.getFieldValue(
                    "dots.single_fieldname.multiple.fieldnames.single_fieldname_again.multiple.fieldnames.again",
                    String.class
                ),
                equalTo("result")
            );

            // Flexible can retrieve list objects
            assertThat(doc.getFieldValue("dots.arrays.dotted.strings", List.class), equalTo(new ArrayList<>(List.of("a", "b", "c", "d"))));
            assertThat(
                doc.getFieldValue("dots.arrays.dotted.objects", List.class),
                equalTo(new ArrayList<>(List.of(new HashMap<>(Map.of("foo", "bar")), new HashMap<>(Map.of("baz", "qux")))))
            );
        });
    }

    public void testNestedGetFieldValueTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("foo.foo.bar", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]")
            );
        }
    }

    public void testListGetFieldValue() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> assertThat(doc.getFieldValue("list.0.field", String.class), equalTo("value")));
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("list.0.field", String.class));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.0.field] is not valid"));
            illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.getFieldValue("dots.arrays.dotted.objects.0.foo", String.class)
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [dots.arrays.dotted.objects.0.foo] is not valid"));
        });
    }

    public void testListGetFieldValueNull() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> assertThat(doc.getFieldValue("list.1", String.class), nullValue()));
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.getFieldValue("list.1", String.class));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.1] is not valid"));
            illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.getFieldValue("dots.arrays.dotted.other.0", String.class)
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [dots.arrays.dotted.other.0] is not valid"));
        });
    }

    public void testListGetFieldValueIndexNotNumeric() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.getFieldValue("list.test.field", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
        try {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.getFieldValue("list.test.field", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.test.field] is not valid"));
        }
        try {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.getFieldValue("dots.arrays.dotted.strings.test.field", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [dots.arrays.dotted.strings.test.field] is not valid"));
        }
    }

    public void testListGetFieldValueIndexOutOfBounds() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.getFieldValue("list.10.field", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }
        try {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.getFieldValue("list.10.field", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.10.field] is not valid"));
        }
        try {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.getFieldValue("dots.arrays.dotted.strings.10", String.class));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [dots.arrays.dotted.strings.10] is not valid"));
        }
    }

    public void testGetFieldValueNotFound() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.getFieldValue("not.here", String.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not] not present as part of path [not.here]"));
        }
        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.getFieldValue("not.here", String.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not.here] not present as part of path [not.here]"));
        }
    }

    public void testGetFieldValueNotFoundNullParent() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("fizz.foo_null.not_there", String.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot resolve [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testGetFieldValueNull() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue(null, String.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testGetFieldValueEmpty() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.getFieldValue("", String.class));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasField() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            assertTrue(doc.hasField("fizz"));
            assertTrue(doc.hasField("_index"));
            assertTrue(doc.hasField("_id"));
            assertTrue(doc.hasField("_source.fizz"));
            assertTrue(doc.hasField("_ingest.timestamp"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> assertTrue(doc.hasField("dotted.bar.baz")));
    }

    public void testHasFieldNested() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            assertTrue(doc.hasField("fizz.buzz"));
            assertTrue(doc.hasField("_source._ingest.timestamp"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            assertTrue(doc.hasField("dots"));
            {
                assertFalse(doc.hasField("dots.foo"));
                assertFalse(doc.hasField("dots.foo.bar"));
                assertTrue(doc.hasField("dots.foo.bar.baz"));
            }

            assertFalse(doc.hasField("dots.dotted"));
            assertTrue(doc.hasField("dots.dotted.integers"));
            {
                assertTrue(doc.hasField("dots.dotted.integers.a"));

                assertFalse(doc.hasField("dots.dotted.integers.b"));
                assertTrue(doc.hasField("dots.dotted.integers.b.c"));

                assertFalse(doc.hasField("dots.dotted.integers.d"));
                assertFalse(doc.hasField("dots.dotted.integers.d.e"));
                assertTrue(doc.hasField("dots.dotted.integers.d.e.f"));

                assertFalse(doc.hasField("dots.dotted.integers.g"));
                assertFalse(doc.hasField("dots.dotted.integers.g.h"));
                assertFalse(doc.hasField("dots.dotted.integers.g.h.i"));
                assertTrue(doc.hasField("dots.dotted.integers.g.h.i.j"));

                assertFalse(doc.hasField("dots.dotted.integers.k"));
                assertFalse(doc.hasField("dots.dotted.integers.k.l"));
                assertFalse(doc.hasField("dots.dotted.integers.k.l.m"));
                assertFalse(doc.hasField("dots.dotted.integers.k.l.m.n"));
                assertTrue(doc.hasField("dots.dotted.integers.k.l.m.n.o"));
            }

            assertTrue(doc.hasField("dots.inaccessible"));
            {
                assertTrue(doc.hasField("dots.inaccessible.a"));
                assertTrue(doc.hasField("dots.inaccessible.a.b"));
                assertTrue(doc.hasField("dots.inaccessible.a.b.c"));
            }

            assertTrue(doc.hasField("dots.arrays"));
            {
                assertTrue(doc.hasField("dots.arrays.dotted.strings"));
                assertTrue(doc.hasField("dots.arrays.dotted.objects"));
            }

            assertTrue(doc.hasField("dots.single_fieldname"));
            {
                assertFalse(doc.hasField("dots.single_fieldname.multiple"));
                assertTrue(doc.hasField("dots.single_fieldname.multiple.fieldnames"));
                assertTrue(doc.hasField("dots.single_fieldname.multiple.fieldnames.single_fieldname_again"));
                assertFalse(doc.hasField("dots.single_fieldname.multiple.fieldnames.single_fieldname_again.multiple"));
                assertFalse(doc.hasField("dots.single_fieldname.multiple.fieldnames.single_fieldname_again.multiple.fieldnames"));
                assertTrue(doc.hasField("dots.single_fieldname.multiple.fieldnames.single_fieldname_again.multiple.fieldnames.again"));
            }

            assertFalse(doc.hasField("dotted.foo.bar.baz.qux"));
            assertTrue(doc.hasField("dotted.foo.bar.baz.qux.quux"));
        });
    }

    public void testListHasField() throws Exception {
        assertTrue(document.hasField("list.0.field"));
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
            // Until then, traversing arrays in the hasFields method returns false
            assertFalse(doc.hasField("dots.arrays.dotted.strings.0"));
            assertFalse(doc.hasField("dots.arrays.dotted.objects.0"));
            assertFalse(doc.hasField("dots.arrays.dotted.objects.0.foo"));
        });
    }

    public void testListHasFieldNull() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> assertTrue(doc.hasField("list.1")));
        // TODO: Flexible will have a new notation for list indexing - For now it does not locate indexed fields
        doWithAccessPattern(FLEXIBLE, (doc) -> assertFalse(doc.hasField("list.1")));
        doWithAccessPattern(FLEXIBLE, (doc) -> assertFalse(doc.hasField("dots.arrays.dotted.other.0")));
    }

    public void testListHasFieldIndexOutOfBounds() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> assertFalse(doc.hasField("list.10")));
        // TODO: Flexible will have a new notation for list indexing - For now it does not locate indexed fields
        doWithAccessPattern(FLEXIBLE, (doc) -> assertFalse(doc.hasField("list.10")));
        doWithAccessPattern(FLEXIBLE, (doc) -> assertFalse(doc.hasField("dots.arrays.dotted.strings.10")));
    }

    public void testListHasFieldIndexOutOfBounds_fail() throws Exception {
        doWithAccessPattern(CLASSIC, doc -> {
            assertTrue(doc.hasField("list.0", true));
            assertTrue(doc.hasField("list.1", true));
            Exception e = expectThrows(IllegalArgumentException.class, () -> doc.hasField("list.2", true));
            assertThat(e.getMessage(), equalTo("[2] is out of bounds for array with length [2] as part of path [list.2]"));
            e = expectThrows(IllegalArgumentException.class, () -> doc.hasField("list.10", true));
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        });
        doWithAccessPattern(FLEXIBLE, doc -> {
            assertFalse(doc.hasField("list.0", true));
            assertFalse(doc.hasField("list.1", true));
            // TODO: Flexible will have a new notation for list indexing - we fail fast, and currently don't check the bounds
            assertFalse(doc.hasField("list.2", true));
            assertFalse(doc.hasField("list.10", true));
        });
    }

    public void testListHasFieldIndexNotNumeric() throws Exception {
        doWithRandomAccessPattern((doc) -> assertFalse(doc.hasField("list.test")));
    }

    public void testNestedHasFieldTypeMismatch() throws Exception {
        doWithRandomAccessPattern((doc) -> assertFalse(doc.hasField("foo.foo.bar")));
    }

    public void testHasFieldNotFound() throws Exception {
        doWithRandomAccessPattern((doc) -> assertFalse(doc.hasField("not.here")));
    }

    public void testHasFieldNotFoundNullParent() throws Exception {
        doWithRandomAccessPattern((doc) -> assertFalse(doc.hasField("fizz.foo_null.not_there")));
    }

    public void testHasFieldNestedNotFound() throws Exception {
        doWithRandomAccessPattern((doc) -> assertFalse(doc.hasField("fizz.doesnotexist")));
    }

    public void testHasFieldNull() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.hasField(null));
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldNullValue() throws Exception {
        doWithRandomAccessPattern((doc) -> assertTrue(doc.hasField("fizz.foo_null")));
        doWithAccessPattern(FLEXIBLE, (doc) -> assertTrue(doc.hasField("dotted.bar.baz_null")));
    }

    public void testHasFieldEmpty() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.hasField(""));
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldSourceObject() throws Exception {
        doWithRandomAccessPattern((doc) -> assertThat(doc.hasField("_source"), equalTo(false)));
    }

    public void testHasFieldIngestObject() throws Exception {
        doWithRandomAccessPattern((doc) -> assertThat(doc.hasField("_ingest"), equalTo(true)));
    }

    public void testHasFieldEmptyPathAfterStrippingOutPrefix() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.hasField("_source."));
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.hasField("_ingest."));
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testSimpleSetFieldValue() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("new_field", "foo");
            assertThat(doc.getSourceAndMetadata().get("new_field"), equalTo("foo"));
            doc.setFieldValue("_ttl", "ttl");
            assertThat(doc.getSourceAndMetadata().get("_ttl"), equalTo("ttl"));
            doc.setFieldValue("_source.another_field", "bar");
            assertThat(doc.getSourceAndMetadata().get("another_field"), equalTo("bar"));
            doc.setFieldValue("_ingest.new_field", "new_value");
            // Metadata contains timestamp, the new_field added above, and the pipeline that is synthesized from doWithRandomAccessPattern
            assertThat(doc.getIngestMetadata().size(), equalTo(3));
            assertThat(doc.getIngestMetadata().get("new_field"), equalTo("new_value"));
            doc.setFieldValue("_ingest.timestamp", "timestamp");
            assertThat(doc.getIngestMetadata().get("timestamp"), equalTo("timestamp"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dotted.bar.buzz", "fizz");
            assertThat(doc.getSourceAndMetadata().get("dotted.bar.buzz"), equalTo("fizz"));
            doc.setFieldValue("_source.dotted.another.buzz", "fizz");
            assertThat(doc.getSourceAndMetadata().get("dotted.another.buzz"), equalTo("fizz"));
            doc.setFieldValue("_ingest.dotted.bar.buzz", "fizz");
            // Metadata contains timestamp, both fields added above, and the pipeline that is synthesized from doWithRandomAccessPattern
            assertThat(doc.getIngestMetadata().size(), equalTo(4));
            assertThat(doc.getIngestMetadata().get("dotted.bar.buzz"), equalTo("fizz"));

            doc.setFieldValue("dotted.foo", "foo");
            assertThat(doc.getSourceAndMetadata().get("dotted.foo"), instanceOf(String.class));
            assertThat(doc.getSourceAndMetadata().get("dotted.foo"), equalTo("foo"));
        });
    }

    public void testSetFieldValueNullValue() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("new_field", (Object) null);
            assertThat(doc.getSourceAndMetadata().containsKey("new_field"), equalTo(true));
            assertThat(doc.getSourceAndMetadata().get("new_field"), nullValue());
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dotted.new.field", (Object) null);
            assertThat(doc.getSourceAndMetadata().containsKey("dotted.new.field"), equalTo(true));
            assertThat(doc.getSourceAndMetadata().get("dotted.new.field"), nullValue());
        });
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetFieldValue() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.setFieldValue("a.b.c.d", "foo");
            assertThat(doc.getSourceAndMetadata().get("a"), instanceOf(Map.class));
            Map<String, Object> a = (Map<String, Object>) doc.getSourceAndMetadata().get("a");
            assertThat(a.get("b"), instanceOf(Map.class));
            Map<String, Object> b = (Map<String, Object>) a.get("b");
            assertThat(b.get("c"), instanceOf(Map.class));
            Map<String, Object> c = (Map<String, Object>) b.get("c");
            assertThat(c.get("d"), instanceOf(String.class));
            String d = (String) c.get("d");
            assertThat(d, equalTo("foo"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dotted.a.b.c.d", "foo");
            assertThat(doc.getSourceAndMetadata().get("dotted.a.b.c.d"), instanceOf(String.class));
            assertThat(doc.getSourceAndMetadata().get("dotted.a.b.c.d"), equalTo("foo"));

            doc.setFieldValue("dotted.foo.bar.baz.blank", "foo");
            assertThat(doc.getSourceAndMetadata().get("dotted.foo.bar.baz"), instanceOf(Map.class));
            Map<String, Object> dottedFooBarBaz = (Map<String, Object>) doc.getSourceAndMetadata().get("dotted.foo.bar.baz");
            assertThat(dottedFooBarBaz.get("blank"), instanceOf(String.class));
            assertThat(dottedFooBarBaz.get("blank"), equalTo("foo"));
        });
    }

    public void testSetFieldValueOnExistingField() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("foo", "newbar");
            assertThat(doc.getSourceAndMetadata().get("foo"), equalTo("newbar"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dotted.bar.baz", "newbaz");
            assertThat(doc.getSourceAndMetadata().get("dotted.bar.baz"), equalTo("newbaz"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testSetFieldValueOnExistingParent() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("fizz.new", "bar");
            assertThat(doc.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
            Map<String, Object> innerMap = (Map<String, Object>) doc.getSourceAndMetadata().get("fizz");
            assertThat(innerMap.get("new"), instanceOf(String.class));
            String value = (String) innerMap.get("new");
            assertThat(value, equalTo("bar"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dots.dotted.integers.new", "qux");
            assertThat(doc.getSourceAndMetadata().get("dots"), instanceOf(Map.class));
            Map<String, Object> innerMap = (Map<String, Object>) doc.getSourceAndMetadata().get("dots");
            assertThat(innerMap.get("dotted.integers"), instanceOf(Map.class));
            Map<String, Object> innermost = (Map<String, Object>) innerMap.get("dotted.integers");
            assertThat(innermost.get("new"), instanceOf(String.class));
            assertThat(innermost.get("new"), equalTo("qux"));
        });
    }

    public void testSetFieldValueOnExistingParentTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue("fizz.buzz.new", "bar"));
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot set [new] with parent object of type [java.lang.String] as part of path [fizz.buzz.new]")
            );
        }
    }

    public void testSetFieldValueOnExistingNullParent() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue("fizz.foo_null.test", "bar"));
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [test] with null parent as part of path [fizz.foo_null.test]"));
        }
    }

    public void testSetFieldValueNullName() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue(null, "bar"));
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testSetSourceObject() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("_source", "value");
            assertThat(doc.getSourceAndMetadata().get("_source"), equalTo("value"));
        });
    }

    public void testSetIngestObject() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("_ingest", "value");
            assertThat(doc.getSourceAndMetadata().get("_ingest"), equalTo("value"));
        });
    }

    public void testSetIngestSourceObject() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            // test that we don't strip out the _source prefix when _ingest is used
            doc.setFieldValue("_ingest._source", "value");
            assertThat(doc.getIngestMetadata().get("_source"), equalTo("value"));
        });
    }

    public void testSetEmptyPathAfterStrippingOutPrefix() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue("_source.", "value"));
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue("_ingest.", "_value"));
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListSetFieldValueNoIndexProvided() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.setFieldValue("list", "value");
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(String.class));
            assertThat(object, equalTo("value"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.setFieldValue("dots.arrays.dotted.strings", "value");
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(String.class));
            assertThat(dottedStringsField, equalTo("value"));
        });
    }

    public void testListAppendFieldValue() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list", "new_value");
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(3));
            assertThat(list.get(0), equalTo(Map.of("field", "value")));
            assertThat(list.get(1), nullValue());
            assertThat(list.get(2), equalTo("new_value"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.strings", "value");
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c", "d", "value")));
        });
    }

    public void testListAppendFieldValueWithDuplicate() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list2", "foo", false);
            Object object = doc.getSourceAndMetadata().get("list2");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(3));
            assertThat(list, equalTo(List.of("foo", "bar", "baz")));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.strings", "a", false);
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c", "d")));
        });
    }

    public void testListAppendFieldValueWithoutDuplicate() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list2", "foo2", false);
            Object object = doc.getSourceAndMetadata().get("list2");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(4));
            assertThat(list, equalTo(List.of("foo", "bar", "baz", "foo2")));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.strings", "e", false);
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c", "d", "e")));
        });
    }

    public void testListAppendFieldValues() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list", List.of("item1", "item2", "item3"));
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(5));
            assertThat(list.get(0), equalTo(Map.of("field", "value")));
            assertThat(list.get(1), nullValue());
            assertThat(list.get(2), equalTo("item1"));
            assertThat(list.get(3), equalTo("item2"));
            assertThat(list.get(4), equalTo("item3"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.strings", List.of("e", "f", "g"));
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c", "d", "e", "f", "g")));
        });
    }

    public void testListAppendFieldValuesWithoutDuplicates() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list2", List.of("foo", "bar", "baz", "foo2"), false);
            Object object = doc.getSourceAndMetadata().get("list2");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(4));
            assertThat(list.get(0), equalTo("foo"));
            assertThat(list.get(1), equalTo("bar"));
            assertThat(list.get(2), equalTo("baz"));
            assertThat(list.get(3), equalTo("foo2"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.strings", List.of("a", "b", "c", "d", "e"), false);
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.strings");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c", "d", "e")));
        });
    }

    public void testAppendFieldValueToNonExistingList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("non_existing_list", "new_value");
            Object object = doc.getSourceAndMetadata().get("non_existing_list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(1));
            assertThat(list.get(0), equalTo("new_value"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.missing", "a");
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.missing");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a")));
        });
    }

    public void testAppendFieldValuesToNonExistingList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("non_existing_list", List.of("item1", "item2", "item3"));
            Object object = doc.getSourceAndMetadata().get("non_existing_list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(3));
            assertThat(list.get(0), equalTo("item1"));
            assertThat(list.get(1), equalTo("item2"));
            assertThat(list.get(2), equalTo("item3"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.missing", List.of("a", "b", "c"));
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arraysField = ((Map<String, Object>) object).get("arrays");
            assertThat(arraysField, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedStringsField = ((Map<String, Object>) arraysField).get("dotted.missing");
            assertThat(dottedStringsField, instanceOf(List.class));
            assertThat(dottedStringsField, equalTo(List.of("a", "b", "c")));
        });
    }

    public void testAppendFieldValueConvertStringToList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("fizz.buzz", "new_value");
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("buzz");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), equalTo("hello world"));
            assertThat(list.get(1), equalTo("new_value"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.foo.bar.baz", "new_value");
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object foobarbaz = ((Map<String, Object>) object).get("foo.bar.baz");
            assertThat(foobarbaz, instanceOf(List.class));
            assertThat(foobarbaz, equalTo(List.of("fizzbuzz", "new_value")));
        });
    }

    public void testAppendFieldValuesConvertStringToList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("fizz.buzz", List.of("item1", "item2", "item3"));
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("buzz");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(4));
            assertThat(list.get(0), equalTo("hello world"));
            assertThat(list.get(1), equalTo("item1"));
            assertThat(list.get(2), equalTo("item2"));
            assertThat(list.get(3), equalTo("item3"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.foo.bar.baz", List.of("fizz", "buzz", "quack"));
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object foobarbaz = ((Map<String, Object>) object).get("foo.bar.baz");
            assertThat(foobarbaz, instanceOf(List.class));
            assertThat(foobarbaz, equalTo(List.of("fizzbuzz", "fizz", "buzz", "quack")));
        });
    }

    public void testAppendFieldValueConvertIntegerToList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            document.appendFieldValue("int", 456);
            Object object = document.getSourceAndMetadata().get("int");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), equalTo(123));
            assertThat(list.get(1), equalTo(456));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.dotted.integers.a", 2);
            Object dots = doc.getSourceAndMetadata().get("dots");
            assertThat(dots, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedIntegers = ((Map<String, Object>) dots).get("dotted.integers");
            assertThat(dottedIntegers, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object a = ((Map<String, Object>) dottedIntegers).get("a");
            assertThat(a, instanceOf(List.class));
            assertThat(a, equalTo(List.of(1, 2)));
        });
    }

    public void testAppendFieldValuesConvertIntegerToList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("int", List.of(456, 789));
            Object object = doc.getSourceAndMetadata().get("int");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(3));
            assertThat(list.get(0), equalTo(123));
            assertThat(list.get(1), equalTo(456));
            assertThat(list.get(2), equalTo(789));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.dotted.integers.a", List.of(2, 3));
            Object dots = doc.getSourceAndMetadata().get("dots");
            assertThat(dots, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedIntegers = ((Map<String, Object>) dots).get("dotted.integers");
            assertThat(dottedIntegers, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object a = ((Map<String, Object>) dottedIntegers).get("a");
            assertThat(a, instanceOf(List.class));
            assertThat(a, equalTo(List.of(1, 2, 3)));
        });
    }

    public void testAppendFieldValueConvertMapToList() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("fizz", Map.of("field", "value"));
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) list.get(0);
            assertThat(map.size(), equalTo(4));
            assertThat(list.get(1), equalTo(Map.of("field", "value")));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.dotted.integers", Map.of("x", "y"));
            Object dots = doc.getSourceAndMetadata().get("dots");
            assertThat(dots, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedIntegers = ((Map<String, Object>) dots).get("dotted.integers");
            assertThat(dottedIntegers, instanceOf(List.class));
            List<?> dottedIntegersList = (List<?>) dottedIntegers;
            assertThat(dottedIntegersList.size(), equalTo(2));
            assertThat(dottedIntegersList.get(0), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> originalMap = (Map<String, Object>) dottedIntegersList.get(0);
            assertThat(originalMap.size(), equalTo(5)); // 5 entries in the original map
            assertThat(dottedIntegersList.get(1), equalTo(Map.of("x", "y")));
        });
    }

    public void testAppendFieldValueToNull() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("fizz.foo_null", "new_value");
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("foo_null");
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), nullValue());
            assertThat(list.get(1), equalTo("new_value"));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dotted.bar.baz_null", "new_value");
            Object object = doc.getSourceAndMetadata().get("dotted.bar.baz_null");
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), nullValue());
            assertThat(list.get(1), equalTo("new_value"));
        });
    }

    public void testAppendFieldValueToListElement() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("fizz.list.0", "item2");
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(1));
            object = list.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<String> innerList = (List<String>) object;
            assertThat(innerList.size(), equalTo(2));
            assertThat(innerList.get(0), equalTo("item1"));
            assertThat(innerList.get(1), equalTo("item2"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.appendFieldValue("dots.arrays.dotted.strings.0", "a1")
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [dots.arrays.dotted.strings.0] is not valid"));
        });
    }

    public void testAppendFieldValuesToListElement() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("fizz.list.0", List.of("item2", "item3", "item4"));
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(1));
            object = list.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<String> innerList = (List<String>) object;
            assertThat(innerList.size(), equalTo(4));
            assertThat(innerList.get(0), equalTo("item1"));
            assertThat(innerList.get(1), equalTo("item2"));
            assertThat(innerList.get(2), equalTo("item3"));
            assertThat(innerList.get(3), equalTo("item4"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.appendFieldValue("dots.arrays.dotted.strings.0", List.of("a1", "a2", "a3"))
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [dots.arrays.dotted.strings.0] is not valid"));
        });
    }

    public void testAppendFieldValueConvertStringListElementToList() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("fizz.list.0.0", "new_value");
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(1));
            object = list.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> innerList = (List<Object>) object;
            object = innerList.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<String> innerInnerList = (List<String>) object;
            assertThat(innerInnerList.size(), equalTo(2));
            assertThat(innerInnerList.get(0), equalTo("item1"));
            assertThat(innerInnerList.get(1), equalTo("new_value"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.appendFieldValue("fizz.list.0.0", "new_value"));
            assertThat(illegalArgument.getMessage(), equalTo("path [fizz.list.0.0] is not valid"));
        });
    }

    public void testAppendFieldValuesConvertStringListElementToList() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("fizz.list.0.0", List.of("item2", "item3", "item4"));
            Object object = doc.getSourceAndMetadata().get("fizz");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            object = map.get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(1));
            object = list.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> innerList = (List<Object>) object;
            object = innerList.get(0);
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<String> innerInnerList = (List<String>) object;
            assertThat(innerInnerList.size(), equalTo(4));
            assertThat(innerInnerList.get(0), equalTo("item1"));
            assertThat(innerInnerList.get(1), equalTo("item2"));
            assertThat(innerInnerList.get(2), equalTo("item3"));
            assertThat(innerInnerList.get(3), equalTo("item4"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.appendFieldValue("fizz.list.0.0", List.of("item2", "item3", "item4"))
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [fizz.list.0.0] is not valid"));
        });
    }

    public void testAppendFieldValueListElementConvertMapToList() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("list.0", Map.of("item2", "value2"));
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), instanceOf(List.class));
            assertThat(list.get(1), nullValue());
            list = (List<?>) list.get(0);
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), equalTo(Map.of("field", "value")));
            assertThat(list.get(1), equalTo(Map.of("item2", "value2")));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(
                IllegalArgumentException.class,
                () -> doc.appendFieldValue("list.0", Map.of("item2", "value2"))
            );
            assertThat(illegalArgument.getMessage(), equalTo("path [list.0] is not valid"));
        });
    }

    public void testAppendFieldValueToNullListElement() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.appendFieldValue("list.1", "new_value");
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            assertThat(list.get(1), instanceOf(List.class));
            list = (List<?>) list.get(1);
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), nullValue());
            assertThat(list.get(1), equalTo("new_value"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.appendFieldValue("list.1", "new_value"));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.1] is not valid"));
        });
    }

    public void testAppendFieldValueToListOfMaps() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.appendFieldValue("list", Map.of("item2", "value2"));
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(3));
            assertThat(list.get(0), equalTo(Map.of("field", "value")));
            assertThat(list.get(1), nullValue());
            assertThat(list.get(2), equalTo(Map.of("item2", "value2")));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.appendFieldValue("dots.arrays.dotted.objects", Map.of("item2", "value2"));
            Object object = doc.getSourceAndMetadata().get("dots");
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object arrays = ((Map<String, Object>) object).get("arrays");
            assertThat(arrays, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Object dottedObjects = ((Map<String, Object>) arrays).get("dotted.objects");
            assertThat(dottedObjects, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) dottedObjects;
            assertThat(list.size(), equalTo(3));
            assertThat(list.get(0), equalTo(Map.of("foo", "bar")));
            assertThat(list.get(1), equalTo(Map.of("baz", "qux")));
            assertThat(list.get(2), equalTo(Map.of("item2", "value2")));
        });
    }

    public void testListSetFieldValueIndexProvided() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.setFieldValue("list.1", "value");
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), equalTo(Map.of("field", "value")));
            assertThat(list.get(1), equalTo("value"));
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.setFieldValue("list.1", "value"));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.1] is not valid"));
        });
    }

    public void testSetFieldValueListAsPartOfPath() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.setFieldValue("list.0.field", "new_value");
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(2));
            assertThat(list.get(0), equalTo(Map.of("field", "new_value")));
            assertThat(list.get(1), nullValue());
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.setFieldValue("list.0.field", "new_value"));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.0.field] is not valid"));
        });
    }

    public void testListSetFieldValueIndexNotNumeric() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.setFieldValue("list.test", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }

        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.setFieldValue("list.test.field", "new_value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.setFieldValue("list.test", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.test] is not valid"));
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.setFieldValue("list.test.field", "new_value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.test.field] is not valid"));
        }
    }

    public void testListSetFieldValueIndexOutOfBounds() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.setFieldValue("list.10", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }

        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.setFieldValue("list.10.field", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.setFieldValue("list.10", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.10] is not valid"));
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.setFieldValue("list.10.field", "value"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.10.field] is not valid"));
        }
    }

    public void testSetFieldValueEmptyName() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.setFieldValue("", "bar"));
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveField() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.removeField("foo");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(14));
            assertThat(doc.getSourceAndMetadata().containsKey("foo"), equalTo(false));
            doc.removeField("_index");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(13));
            assertThat(doc.getSourceAndMetadata().containsKey("_index"), equalTo(false));
            doc.removeField("_source.fizz");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(12));
            assertThat(doc.getSourceAndMetadata().containsKey("fizz"), equalTo(false));
            assertThat(doc.getIngestMetadata().size(), equalTo(2));
            doc.removeField("_ingest.timestamp");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(12));
            assertThat(doc.getIngestMetadata().size(), equalTo(1));
            doc.removeField("_ingest.pipeline");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(12));
            assertThat(doc.getIngestMetadata().size(), equalTo(0));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.removeField("dotted.bar.baz");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(11));
            assertThat(doc.getSourceAndMetadata().containsKey("dotted.bar.baz"), equalTo(false));
        });
    }

    public void testRemoveFieldIgnoreMissing() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.removeField("foo", randomBoolean());
            assertThat(doc.getSourceAndMetadata().size(), equalTo(14));
            assertThat(doc.getSourceAndMetadata().containsKey("foo"), equalTo(false));
            doc.removeField("_index", randomBoolean());
            assertThat(doc.getSourceAndMetadata().size(), equalTo(13));
            assertThat(doc.getSourceAndMetadata().containsKey("_index"), equalTo(false));
        });

        // if ignoreMissing is false, we throw an exception for values that aren't found
        switch (randomIntBetween(0, 2)) {
            case 0 -> doWithRandomAccessPattern((doc) -> {
                doc.setFieldValue("fizz.some", (Object) null);
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> doc.removeField("fizz.some.nonsense", false)
                );
                assertThat(e.getMessage(), is("cannot remove [nonsense] from null as part of path [fizz.some.nonsense]"));
            });
            case 1 -> {
                // Different error messages for each access pattern when trying to remove an element from a list incorrectly
                doWithAccessPattern(CLASSIC, (doc) -> {
                    doc.setFieldValue("fizz.some", List.of("foo", "bar"));
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> doc.removeField("fizz.some.nonsense", false)
                    );
                    assertThat(
                        e.getMessage(),
                        is("[nonsense] is not an integer, cannot be used as an index as part of path [fizz.some.nonsense]")
                    );
                });
                doWithAccessPattern(FLEXIBLE, (doc) -> {
                    doc.setFieldValue("fizz.other", List.of("foo", "bar"));
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> doc.removeField("fizz.other.nonsense", false)
                    );
                    assertThat(e.getMessage(), is("path [fizz.other.nonsense] is not valid"));
                });
            }
            case 2 -> {
                // Different error messages when removing a nested field that does not exist
                doWithAccessPattern(CLASSIC, (doc) -> {
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> doc.removeField("fizz.some.nonsense", false)
                    );
                    assertThat(e.getMessage(), is("field [some] not present as part of path [fizz.some.nonsense]"));
                });
                doWithAccessPattern(FLEXIBLE, (doc) -> {
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> doc.removeField("fizz.some.nonsense", false)
                    );
                    assertThat(e.getMessage(), is("field [some.nonsense] not present as part of path [fizz.some.nonsense]"));
                });
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }

        // but no exception is thrown if ignoreMissing is true
        doWithRandomAccessPattern((doc) -> doc.removeField("fizz.some.nonsense", true));
    }

    public void testRemoveInnerField() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.removeField("fizz.buzz");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) doc.getSourceAndMetadata().get("fizz");
            assertThat(map.size(), equalTo(3));
            assertThat(map.containsKey("buzz"), equalTo(false));

            doc.removeField("fizz.foo_null");
            assertThat(map.size(), equalTo(2));
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

            doc.removeField("fizz.1");
            assertThat(map.size(), equalTo(1));
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

            doc.removeField("fizz.list");
            assertThat(map.size(), equalTo(0));
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("fizz"), equalTo(true));
        });
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            doc.removeField("dots.foo.bar.baz");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().get("dots"), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> dots = (Map<String, Object>) doc.getSourceAndMetadata().get("dots");
            assertThat(dots.size(), equalTo(5));
            assertThat(dots.containsKey("foo.bar.baz"), equalTo(false));

            doc.removeField("dots.foo.bar.null");
            assertThat(dots.size(), equalTo(4));
            assertThat(dots.containsKey("foo.bar.null"), equalTo(false));
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("dots"), equalTo(true));

            doc.removeField("dots.arrays.dotted.strings");
            @SuppressWarnings("unchecked")
            Map<String, Object> arrays = (Map<String, Object>) dots.get("arrays");
            assertThat(dots.size(), equalTo(4));
            assertThat(arrays.size(), equalTo(2));
            assertThat(arrays.containsKey("dotted.strings"), equalTo(false));
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("dots"), equalTo(true));
        });
    }

    public void testRemoveNonExistingField() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("does_not_exist"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does_not_exist] not present as part of path [does_not_exist]"));
        }

        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.removeField("does.not.exist"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does] not present as part of path [does.not.exist]"));
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("does.not.exist"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does.not.exist] not present as part of path [does.not.exist]"));
        }
    }

    public void testRemoveExistingParentTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("foo.foo.bar"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]")
            );
        }

        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("dots.foo.bar.baz.qux.quux"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [qux] from object of type [java.lang.String] as part of path [dots.foo.bar.baz.qux.quux]")
            );
        }
    }

    public void testRemoveSourceObject() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("_source"));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testRemoveIngestObject() throws Exception {
        doWithRandomAccessPattern((doc) -> {
            doc.removeField("_ingest");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(14));
            assertThat(doc.getSourceAndMetadata().containsKey("_ingest"), equalTo(false));
        });
    }

    public void testRemoveEmptyPathAfterStrippingOutPrefix() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("_source."));
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("_ingest."));
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListRemoveField() throws Exception {
        doWithAccessPattern(CLASSIC, (doc) -> {
            doc.removeField("list.0.field");
            assertThat(doc.getSourceAndMetadata().size(), equalTo(15));
            assertThat(doc.getSourceAndMetadata().containsKey("list"), equalTo(true));
            Object object = doc.getSourceAndMetadata().get("list");
            assertThat(object, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            assertThat(list.size(), equalTo(2));
            object = list.get(0);
            assertThat(object, instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            assertThat(map.size(), equalTo(0));
            document.removeField("list.0");
            assertThat(list.size(), equalTo(1));
            assertThat(list.get(0), nullValue());
        });
        // TODO: Flexible will have a new notation for list indexing - For now it does not support traversing lists
        doWithAccessPattern(FLEXIBLE, (doc) -> {
            var illegalArgument = expectThrows(IllegalArgumentException.class, () -> doc.removeField("list.0.field"));
            assertThat(illegalArgument.getMessage(), equalTo("path [list.0.field] is not valid"));
        });
    }

    public void testRemoveFieldValueNotFoundNullParent() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("fizz.foo_null.not_there"));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("dots.foo.bar.null.not_there"));
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [not_there] from null as part of path [dots.foo.bar.null.not_there]"));
        }
    }

    public void testNestedRemoveFieldTypeMismatch() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField("fizz.1.bar"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [bar] from object of type [java.lang.String] as part of path [fizz.1.bar]"));
        }
        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("dots.dotted.integers.a.bar.baz"));
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [bar] from object of type [java.lang.Integer] as part of path [dots.dotted.integers.a.bar.baz]")
            );
        }
    }

    public void testListRemoveFieldIndexNotNumeric() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.removeField("list.test"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }
        // Flexible mode does not allow for interactions with arrays yet
        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("list.test"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.test] is not valid"));
        }
    }

    public void testListRemoveFieldIndexOutOfBounds() throws Exception {
        try {
            doWithAccessPattern(CLASSIC, (doc) -> doc.removeField("list.10"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }
        // Flexible mode does not allow for interactions with arrays yet
        try {
            doWithAccessPattern(FLEXIBLE, (doc) -> doc.removeField("list.10"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [list.10] is not valid"));
        }
    }

    public void testRemoveNullField() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField(null));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveEmptyField() throws Exception {
        try {
            doWithRandomAccessPattern((doc) -> doc.removeField(""));
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testIngestMetadataTimestamp() {
        long before = System.currentTimeMillis();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        long after = System.currentTimeMillis();
        ZonedDateTime timestamp = (ZonedDateTime) ingestDocument.getIngestMetadata().get(IngestDocument.TIMESTAMP);
        long actualMillis = timestamp.toInstant().toEpochMilli();
        assertThat(timestamp, notNullValue());
        assertThat(actualMillis, greaterThanOrEqualTo(before));
        assertThat(actualMillis, lessThanOrEqualTo(after));
    }

    public void testCopyConstructor() {
        {
            // generic test with a random document and copy
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            IngestDocument copy = new IngestDocument(ingestDocument);

            // these fields should not be the same instance
            assertThat(ingestDocument.getSourceAndMetadata(), not(sameInstance(copy.getSourceAndMetadata())));
            assertThat(ingestDocument.getCtxMap(), not(sameInstance(copy.getCtxMap())));
            assertThat(ingestDocument.getCtxMap().getMetadata(), not(sameInstance(copy.getCtxMap().getMetadata())));

            // but the two objects should be very much equal to each other
            assertIngestDocument(ingestDocument, copy);
        }

        {
            // manually punch in a few values
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            ingestDocument.setFieldValue("_index", "foo1");
            ingestDocument.setFieldValue("_id", "bar1");
            ingestDocument.setFieldValue("hello", "world1");
            IngestDocument copy = new IngestDocument(ingestDocument);

            // make sure the copy matches
            assertIngestDocument(ingestDocument, copy);

            // change the copy
            copy.setFieldValue("_index", "foo2");
            copy.setFieldValue("_id", "bar2");
            copy.setFieldValue("hello", "world2");

            // the original shouldn't have changed
            assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("foo1"));
            assertThat(ingestDocument.getFieldValue("_id", String.class), equalTo("bar1"));
            assertThat(ingestDocument.getFieldValue("hello", String.class), equalTo("world1"));
        }

        {
            // the copy constructor rejects self-references
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            List<Object> someList = new ArrayList<>();
            someList.add("some string");
            someList.add(someList); // the list contains itself
            ingestDocument.setFieldValue("someList", someList);
            Exception e = expectThrows(IllegalArgumentException.class, () -> new IngestDocument(ingestDocument));
            assertThat(e.getMessage(), equalTo("Iterable object is self-referencing itself"));
        }
    }

    public void testCopyConstructorWithExecutedPipelines() {
        /*
         * This is similar to the first part of testCopyConstructor, except that we're executing a pipeilne, and running the
         * assertions inside the processor so that we can test that executedPipelines is correct.
         */
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        TestProcessor processor = new TestProcessor(ingestDocument1 -> {
            assertThat(ingestDocument1.getPipelineStack().size(), equalTo(1));
            IngestDocument copy = new IngestDocument(ingestDocument1);
            assertThat(ingestDocument1.getSourceAndMetadata(), not(sameInstance(copy.getSourceAndMetadata())));
            assertThat(ingestDocument1.getCtxMap(), not(sameInstance(copy.getCtxMap())));
            assertThat(ingestDocument1.getCtxMap().getMetadata(), not(sameInstance(copy.getCtxMap().getMetadata())));
            assertIngestDocument(ingestDocument1, copy);
            assertThat(copy.getPipelineStack(), equalTo(ingestDocument1.getPipelineStack()));
        });
        Pipeline pipeline = new Pipeline("pipeline1", "test pipeline", 1, Map.of(), new CompoundProcessor(processor));
        ingestDocument.executePipeline(pipeline, (ingestDocument1, exception) -> {
            assertNotNull(ingestDocument1);
            assertNull(exception);
        });
        assertThat(processor.getInvokedCounter(), equalTo(1));
    }

    public void testCopyConstructorWithZonedDateTime() {
        ZoneId timezone = ZoneId.of("Europe/London");

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("beforeClockChange", ZonedDateTime.ofInstant(Instant.ofEpochSecond(1509237000), timezone));
        sourceAndMetadata.put("afterClockChange", ZonedDateTime.ofInstant(Instant.ofEpochSecond(1509240600), timezone));

        IngestDocument original = TestIngestDocument.withDefaultVersion(sourceAndMetadata);
        IngestDocument copy = new IngestDocument(original);

        assertThat(copy.getSourceAndMetadata().get("beforeClockChange"), equalTo(original.getSourceAndMetadata().get("beforeClockChange")));
        assertThat(copy.getSourceAndMetadata().get("afterClockChange"), equalTo(original.getSourceAndMetadata().get("afterClockChange")));
    }

    public void testSetInvalidSourceField() {
        Map<String, Object> document = new HashMap<>();
        Object randomObject = randomFrom(new ArrayList<>(), new HashMap<>(), 12, 12.34);
        document.put("source_field", randomObject);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        try {
            ingestDocument.getFieldValueAsBytes("source_field");
            fail("Expected an exception due to invalid source field, but did not happen");
        } catch (IllegalArgumentException e) {
            String expectedClassName = randomObject.getClass().getName();

            assertThat(
                e.getMessage(),
                containsString("field [source_field] of unknown type [" + expectedClassName + "], must be string or byte array")
            );
        }
    }

    public void testDeepCopy() {
        IngestDocument copiedDoc = new IngestDocument(
            IngestDocument.deepCopyMap(document.getSourceAndMetadata()),
            IngestDocument.deepCopyMap(document.getIngestMetadata())
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            document.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            1e-10
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class),
            document.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class)
        );
    }

    public void testGetAllFields() {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("_id", "a123");
        source.put("name", "eric clapton");
        source.put("address", address);
        source.put("name.display", "Eric Clapton");

        Set<String> result = IngestDocument.getAllFields(source);

        assertThat(result, containsInAnyOrder("_id", "name", "address", "address.street", "address.number", "name.display"));
    }

    public void testIsMetadata() {
        assertTrue(IngestDocument.Metadata.isMetadata("_type"));
        assertTrue(IngestDocument.Metadata.isMetadata("_index"));
        assertTrue(IngestDocument.Metadata.isMetadata("_version"));
        assertFalse(IngestDocument.Metadata.isMetadata("name"));
        assertFalse(IngestDocument.Metadata.isMetadata("address"));
    }

    public void testIndexHistory() {
        // the index history contains the original index
        String index1 = document.getFieldValue("_index", String.class);
        assertThat(index1, equalTo("index"));
        assertThat(document.getIndexHistory(), Matchers.contains(index1));

        // it can be updated to include another index
        String index2 = "another_index";
        assertTrue(document.updateIndexHistory(index2));
        assertThat(document.getIndexHistory(), Matchers.contains(index1, index2));

        // an index cycle cannot be introduced, however
        assertFalse(document.updateIndexHistory(index1));
        assertThat(document.getIndexHistory(), Matchers.contains(index1, index2));
    }

    public void testSourceHashMapIsNotCopied() {
        // an ingest document's ctxMap will, as an optimization, just use the passed-in map reference
        {
            Map<String, Object> source = new HashMap<>(Map.of("foo", 1));
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        {
            Map<String, Object> source = XContentHelper.convertToMap(new BytesArray("{ \"foo\": 1 }"), false, XContentType.JSON).v2();
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        {
            Map<String, Object> source = Map.of("foo", 1);
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        // a cloned ingest document will copy the map, though
        {
            Map<String, Object> source = Map.of("foo", 1);
            IngestDocument document1 = new IngestDocument("index", "id", 1, null, null, source);
            document1.getIngestMetadata().put("bar", 2);
            IngestDocument document2 = new IngestDocument(document1);
            assertThat(document2.getCtxMap().getMetadata(), equalTo(document1.getCtxMap().getMetadata()));
            assertThat(document2.getSource(), not(sameInstance(source)));
            assertThat(document2.getCtxMap().getMetadata(), equalTo(document1.getCtxMap().getMetadata()));
            assertThat(document2.getCtxMap().getSource(), not(sameInstance(source)));

            // it also copies these other nearby maps
            assertThat(document2.getIngestMetadata(), equalTo(document1.getIngestMetadata()));
            assertThat(document2.getIngestMetadata(), not(sameInstance(document1.getIngestMetadata())));

            assertThat(document2.getCtxMap().getMetadata(), not(sameInstance(document1.getCtxMap().getMetadata())));
            assertThat(document2.getCtxMap().getMetadata(), not(sameInstance(document1.getCtxMap().getMetadata())));
        }
    }

    /**
     * When executing nested pipelines on an ingest document, the document should keep track of each pipeline's access pattern for the
     * lifetime of each pipeline execution. When a pipeline execution concludes, it should clear access pattern from the document and
     * restore the previous pipeline's access pattern.
     */
    public void testNestedAccessPatternPropagation() {
        Map<String, Object> source = new HashMap<>(Map.of("foo", 1));
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        // 1-3 nested calls
        doTestNestedAccessPatternPropagation(0, randomIntBetween(1, 5), document);

        // At the end of the test, there should be neither pipeline ids nor access patterns left in the stack.
        assertThat(document.getPipelineStack(), is(empty()));
        assertThat(document.getCurrentAccessPattern().isEmpty(), is(true));
    }

    /**
     * Recursively execute some number of pipelines at various call depths to simulate a robust chain of pipelines being called on a
     * document.
     * @param level The current call depth. This is how many pipelines deep into the nesting we are.
     * @param maxCallDepth How much further in the call depth we should go in the test. If this is greater than the current level, we will
     *                     recurse in at least one of the pipelines executed at this level. If the current level is equal to the max call
     *                     depth we will run some pipelines but recurse no further before returning.
     * @param document The document to repeatedly use and verify against.
     */
    void doTestNestedAccessPatternPropagation(int level, int maxCallDepth, IngestDocument document) {
        // 1-5 pipelines to be run at any given level
        logger.debug("LEVEL {}/{}: BEGIN", level, maxCallDepth);
        int pipelinesAtThisLevel = randomIntBetween(1, 7);
        logger.debug("Run pipelines: {}", pipelinesAtThisLevel);

        boolean recursed = false;
        if (level >= maxCallDepth) {
            // If we're at max call depth, do no recursions
            recursed = true;
            logger.debug("No more recursions");
        }

        for (int pipelineIdx = 0; pipelineIdx < pipelinesAtThisLevel; pipelineIdx++) {
            String expectedPipelineId = randomAlphaOfLength(20);
            IngestPipelineFieldAccessPattern expectedAccessPattern = randomFrom(IngestPipelineFieldAccessPattern.values());

            // We mock the pipeline because it's easier to verify calls and doesn't
            // need us to force a stall in the execution logic to half apply it.
            Pipeline mockPipeline = mock(Pipeline.class);
            when(mockPipeline.getId()).thenReturn(expectedPipelineId);
            when(mockPipeline.getProcessors()).thenReturn(List.of(new TestProcessor((doc) -> {})));
            when(mockPipeline.getFieldAccessPattern()).thenReturn(expectedAccessPattern);
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> mockHandler = mock(BiConsumer.class);

            // Execute pipeline
            logger.debug("LEVEL {}/{}: Executing {}/{}", level, maxCallDepth, pipelineIdx, pipelinesAtThisLevel);
            document.executePipeline(mockPipeline, mockHandler);

            // Verify pipeline was called, capture completion handler
            ArgumentCaptor<BiConsumer<IngestDocument, Exception>> argumentCaptor = ArgumentCaptor.captor();
            verify(mockPipeline).execute(eq(document), argumentCaptor.capture());

            // Assert expected state
            assertThat(document.getPipelineStack().getFirst(), is(expectedPipelineId));
            assertThat(document.getCurrentAccessPattern().isPresent(), is(true));
            assertThat(document.getCurrentAccessPattern().get(), is(expectedAccessPattern));

            // Randomly recurse: We recurse only one time per level to avoid hogging test time, but we randomize which
            // pipeline to recurse on, eventually requiring a recursion on the last pipeline run if one hasn't happened yet.
            if (recursed == false && (randomBoolean() || pipelineIdx == pipelinesAtThisLevel - 1)) {
                logger.debug("Recursed on pipeline {}", pipelineIdx);
                doTestNestedAccessPatternPropagation(level + 1, maxCallDepth, document);
                recursed = true;
            }

            // Pull up the captured completion handler to conclude the pipeline run
            argumentCaptor.getValue().accept(document, null);

            // Assert expected state
            assertThat(document.getPipelineStack().size(), is(equalTo(level)));
            if (level == 0) {
                // Top level means access pattern should be empty
                assertThat(document.getCurrentAccessPattern().isEmpty(), is(true));
            } else {
                // If we're nested below the top level we should still have an access
                // pattern on the document for the pipeline above us
                assertThat(document.getCurrentAccessPattern().isPresent(), is(true));
            }
        }
        logger.debug("LEVEL {}/{}: COMPLETE", level, maxCallDepth);
    }

    @SuppressWarnings("unchecked")
    public void testGetUnmodifiableSourceAndMetadata() {
        assertMutatingThrows(ctx -> ctx.remove("foo"));
        assertMutatingThrows(ctx -> ctx.put("foo", "bar"));
        assertMutatingThrows(ctx -> ((List<Object>) ctx.get("listField")).add("bar"));
        assertMutatingThrows(ctx -> ((List<Object>) ctx.get("listField")).remove("bar"));
        assertMutatingThrows(ctx -> ((Set<Object>) ctx.get("setField")).add("bar"));
        assertMutatingThrows(ctx -> ((Set<Object>) ctx.get("setField")).remove("bar"));
        assertMutatingThrows(ctx -> ((Map<String, Object>) ctx.get("mapField")).put("bar", "baz"));
        assertMutatingThrows(ctx -> ((Map<?, ?>) ctx.get("mapField")).remove("bar"));
        assertMutatingThrows(ctx -> ((List<Object>) ((Set<Object>) ctx.get("setField")).iterator().next()).add("bar"));
        assertMutatingThrows(
            ctx -> ((List<Object>) ((List<Object>) ((Set<Object>) ctx.get("setField")).iterator().next()).iterator().next()).add("bar")
        );

        /*
         * The source can also have a byte array. But we do not throw an UnsupportedOperationException when a byte array is changed --
         * we just ignore the change.
         */
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue("byteArrayField", randomByteArrayOfLength(10));
        Map<String, Object> unmodifiableDocument = ingestDocument.getUnmodifiableSourceAndMetadata();
        byte originalByteValue = ((byte[]) unmodifiableDocument.get("byteArrayField"))[0];
        ((byte[]) unmodifiableDocument.get("byteArrayField"))[0] = (byte) (originalByteValue + 1);
        assertThat(((byte[]) unmodifiableDocument.get("byteArrayField"))[0], equalTo(originalByteValue));
    }

    @SuppressWarnings("unchecked")
    public void assertMutatingThrows(Consumer<Map<String, Object>> mutation) {
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue("listField", new ArrayList<>());
        ingestDocument.setFieldValue("mapField", new HashMap<>());
        ingestDocument.setFieldValue("setField", new HashSet<>());
        List<Object> listWithinSet = new ArrayList<>();
        listWithinSet.add(new ArrayList<>());
        ingestDocument.getFieldValue("setField", Set.class).add(listWithinSet);
        Map<String, Object> unmodifiableDocument = ingestDocument.getUnmodifiableSourceAndMetadata();
        assertThrows(UnsupportedOperationException.class, () -> mutation.accept(unmodifiableDocument));
        mutation.accept(ingestDocument.getSourceAndMetadata()); // no exception expected
    }
}
