/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.searchafter;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.function.BiFunction;

import static org.elasticsearch.search.searchafter.SearchAfterBuilder.extractSortType;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchAfterBuilderTests extends ESTestCase {
    private static final int NUMBER_OF_TESTBUILDERS = 20;

    /**
     * Generates a random {@link SearchAfterBuilder}.
     */
    public static SearchAfterBuilder randomSearchAfterBuilder() throws IOException {
        int numSearchFrom = randomIntBetween(1, 10);
        SearchAfterBuilder searchAfterBuilder = new SearchAfterBuilder();
        Object[] values = new Object[numSearchFrom];
        for (int i = 0; i < numSearchFrom; i++) {
            int branch = randomInt(10);
            switch (branch) {
                case 0 -> values[i] = randomInt();
                case 1 -> values[i] = randomFloat();
                case 2 -> values[i] = randomLong();
                case 3 -> values[i] = randomDouble();
                case 4 -> values[i] = randomAlphaOfLengthBetween(5, 20);
                case 5 -> values[i] = randomBoolean();
                case 6 -> values[i] = randomByte();
                case 7 -> values[i] = randomShort();
                case 8 -> values[i] = new Text(randomAlphaOfLengthBetween(5, 20));
                case 9 -> values[i] = null;
                case 10 -> values[i] = randomBigInteger();
            }
        }
        searchAfterBuilder.setSortValues(values);
        return searchAfterBuilder;
    }

    /**
     * We build a json version of the search_after first in order to
     * ensure that every number type remain the same before/after xcontent (de)serialization.
     * This is not a problem because the final type of each field value is extracted from associated sort field.
     * This little trick ensure that equals and hashcode are the same when using the xcontent serialization.
     */
    public static SearchAfterBuilder randomJsonSearchFromBuilder(BiFunction<XContent, BytesReference, XContentParser> createParser)
        throws IOException {
        int numSearchAfter = randomIntBetween(1, 10);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.startArray("search_after");
        for (int i = 0; i < numSearchAfter; i++) {
            int branch = randomInt(9);
            switch (branch) {
                case 0 -> jsonBuilder.value(randomInt());
                case 1 -> jsonBuilder.value(randomFloat());
                case 2 -> jsonBuilder.value(randomLong());
                case 3 -> jsonBuilder.value(randomDouble());
                case 4 -> jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                case 5 -> jsonBuilder.value(randomBoolean());
                case 6 -> jsonBuilder.value(randomByte());
                case 7 -> jsonBuilder.value(randomShort());
                case 8 -> jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                case 9 -> jsonBuilder.nullValue();
            }
        }
        jsonBuilder.endArray();
        jsonBuilder.endObject();
        try (XContentParser parser = createParser.apply(JsonXContent.jsonXContent, BytesReference.bytes(jsonBuilder))) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            return SearchAfterBuilder.fromXContent(parser);
        }
    }

    private static SearchAfterBuilder serializedCopy(SearchAfterBuilder original) throws IOException {
        return copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), SearchAfterBuilder::new);
    }

    public void testSerialization() throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SearchAfterBuilder original = randomSearchAfterBuilder();
            SearchAfterBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    public void testEqualsAndHashcode() throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            // TODO add equals tests with mutating the original object
            checkEqualsAndHashCode(randomSearchAfterBuilder(), SearchAfterBuilderTests::serializedCopy);
        }
    }

    public void testFromXContent() throws Exception {
        for (int runs = 0; runs < 20; runs++) {
            SearchAfterBuilder searchAfterBuilder = randomJsonSearchFromBuilder((xContent, data) -> {
                try {
                    return createParser(xContent, data);
                } catch (IOException ioe) {
                    throw new UncheckedIOException(ioe);
                }
            });
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            builder.startObject();
            searchAfterBuilder.innerToXContent(builder);
            builder.endObject();
            try (XContentParser parser = createParser(shuffleXContent(builder))) {
                parser.nextToken();
                parser.nextToken();
                parser.nextToken();
                SearchAfterBuilder secondSearchAfterBuilder = SearchAfterBuilder.fromXContent(parser);
                assertNotSame(searchAfterBuilder, secondSearchAfterBuilder);
                assertEquals(searchAfterBuilder, secondSearchAfterBuilder);
                assertEquals(searchAfterBuilder.hashCode(), secondSearchAfterBuilder.hashCode());
            }
        }
    }

    public void testFromXContentIllegalType() throws Exception {
        for (XContentType type : XContentType.values()) {
            // BIG_DECIMAL
            // ignore json and yaml, they parse floating point numbers as floats/doubles
            if (type.canonical() == XContentType.JSON || type.canonical() == XContentType.YAML) {
                continue;
            }
            XContentBuilder xContent = XContentFactory.contentBuilder(type);
            xContent.startObject().startArray("search_after").value(new BigDecimal("9223372036854776003.3")).endArray().endObject();
            try (XContentParser parser = createParser(xContent)) {
                parser.nextToken();
                parser.nextToken();
                parser.nextToken();
                IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> SearchAfterBuilder.fromXContent(parser));
                assertThat(exc.getMessage(), containsString("BIG_DECIMAL"));
            }
        }
    }

    public void testWithNullArray() throws Exception {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        try {
            builder.setSortValues(null);
            fail("Should fail on null array.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("Values cannot be null."));
        }
    }

    public void testWithEmptyArray() throws Exception {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        try {
            builder.setSortValues(new Object[0]);
            fail("Should fail on empty array.");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Values must contains at least one value."));
        }
    }

    /**
     * Explicitly tests what you can't list as a sortValue. What you can list is tested by {@link #randomSearchAfterBuilder()}.
     */
    public void testBadTypes() throws IOException {
        randomSearchFromBuilderWithSortValueThrows(new Object());
        randomSearchFromBuilderWithSortValueThrows(new GeoPoint(0, 0));
        randomSearchFromBuilderWithSortValueThrows(randomSearchAfterBuilder());
        randomSearchFromBuilderWithSortValueThrows(this);
    }

    private static void randomSearchFromBuilderWithSortValueThrows(Object containing) throws IOException {
        // Get a valid one
        SearchAfterBuilder builder = randomSearchAfterBuilder();
        // Now replace its values with one containing the passed in object
        Object[] values = builder.getSortValues();
        values[between(0, values.length - 1)] = containing;
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.setSortValues(values));
        assertEquals(e.getMessage(), "Can't handle search_after field value of type [" + containing.getClass() + "]");
    }

    public void testExtractSortType() throws Exception {
        SortField.Type type = extractSortType(LatLonDocValuesField.newDistanceSort("field", 0.0, 180.0));
        assertThat(type, equalTo(SortField.Type.DOUBLE));
        IndexFieldData.XFieldComparatorSource source = new IndexFieldData.XFieldComparatorSource(null, MultiValueMode.MIN, null) {
            @Override
            public SortField.Type reducedType() {
                return SortField.Type.STRING;
            }

            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
                return null;
            }

            @Override
            public BucketedSort newBucketedSort(
                BigArrays bigArrays,
                SortOrder sortOrder,
                DocValueFormat format,
                int bucketSize,
                BucketedSort.ExtraData extra
            ) {
                return null;
            }
        };

        type = extractSortType(new SortField("field", source));
        assertThat(type, equalTo(SortField.Type.STRING));

        type = extractSortType(new SortedNumericSortField("field", SortField.Type.DOUBLE));
        assertThat(type, equalTo(SortField.Type.DOUBLE));

        type = extractSortType(new SortedSetSortField("field", false));
        assertThat(type, equalTo(SortField.Type.STRING));
    }

    public void testBuildFieldDocWithCollapse() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchAfterBuilder.buildFieldDoc(
                new SortAndFormats(new Sort(), new DocValueFormat[] { DocValueFormat.RAW }),
                new Object[] { 1 },
                "collapse_field"
            )
        );
        assertThat(e.getMessage(), containsString("Cannot use [collapse] in conjunction with"));

        FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(
            new SortAndFormats(
                new Sort(new SortField("collapse_field", SortField.Type.STRING)),
                new DocValueFormat[] { DocValueFormat.RAW }
            ),
            new Object[] { "foo" },
            "collapse_field"
        );
        assertEquals(fieldDoc.toString(), new FieldDoc(Integer.MAX_VALUE, 0, new Object[] { new BytesRef("foo") }).toString());
    }
}
