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

package org.elasticsearch.search.searchafter;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;

import static org.elasticsearch.search.searchafter.SearchAfterBuilder.extractSortType;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchAfterBuilderTests extends ESTestCase {
    private static final int NUMBER_OF_TESTBUILDERS = 20;

    private static SearchAfterBuilder randomSearchAfterBuilder() throws IOException {
        int numSearchFrom = randomIntBetween(1, 10);
        SearchAfterBuilder searchAfterBuilder = new SearchAfterBuilder();
        Object[] values = new Object[numSearchFrom];
        for (int i = 0; i < numSearchFrom; i++) {
            int branch = randomInt(9);
            switch (branch) {
                case 0:
                    values[i] = randomInt();
                    break;
                case 1:
                    values[i] = randomFloat();
                    break;
                case 2:
                    values[i] = randomLong();
                    break;
                case 3:
                    values[i] = randomDouble();
                    break;
                case 4:
                    values[i] = randomAlphaOfLengthBetween(5, 20);
                    break;
                case 5:
                    values[i] = randomBoolean();
                    break;
                case 6:
                    values[i] = randomByte();
                    break;
                case 7:
                    values[i] = randomShort();
                    break;
                case 8:
                    values[i] = new Text(randomAlphaOfLengthBetween(5, 20));
                    break;
                case 9:
                    values[i] = null;
                    break;
            }
        }
        searchAfterBuilder.setSortValues(values);
        return searchAfterBuilder;
    }

    // We build a json version of the search_after first in order to
    // ensure that every number type remain the same before/after xcontent (de)serialization.
    // This is not a problem because the final type of each field value is extracted from associated sort field.
    // This little trick ensure that equals and hashcode are the same when using the xcontent serialization.
    private SearchAfterBuilder randomJsonSearchFromBuilder() throws IOException {
        int numSearchAfter = randomIntBetween(1, 10);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.startArray("search_after");
        for (int i = 0; i < numSearchAfter; i++) {
            int branch = randomInt(9);
            switch (branch) {
                case 0:
                    jsonBuilder.value(randomInt());
                    break;
                case 1:
                    jsonBuilder.value(randomFloat());
                    break;
                case 2:
                    jsonBuilder.value(randomLong());
                    break;
                case 3:
                    jsonBuilder.value(randomDouble());
                    break;
                case 4:
                    jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                    break;
                case 5:
                    jsonBuilder.value(randomBoolean());
                    break;
                case 6:
                    jsonBuilder.value(randomByte());
                    break;
                case 7:
                    jsonBuilder.value(randomShort());
                    break;
                case 8:
                    jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                    break;
                case 9:
                    jsonBuilder.nullValue();
                    break;
            }
        }
        jsonBuilder.endArray();
        jsonBuilder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(jsonBuilder))) {
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
            SearchAfterBuilder searchAfterBuilder = randomJsonSearchFromBuilder();
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
            // BIG_INTEGER
            XContentBuilder xContent = XContentFactory.contentBuilder(type);
            xContent.startObject()
                .startArray("search_after")
                .value(new BigInteger("9223372036854776000"))
                .endArray()
                .endObject();
            try (XContentParser parser = createParser(xContent)) {
                parser.nextToken();
                parser.nextToken();
                parser.nextToken();
                IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> SearchAfterBuilder.fromXContent(parser));
                assertThat(exc.getMessage(), containsString("BIG_INTEGER"));
            }

            // BIG_DECIMAL
            // ignore json and yaml, they parse floating point numbers as floats/doubles
            if (type == XContentType.JSON || type == XContentType.YAML) {
                continue;
            }
            xContent = XContentFactory.contentBuilder(type);
            xContent.startObject()
                .startArray("search_after")
                    .value(new BigDecimal("9223372036854776003.3"))
                .endArray()
                .endObject();
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
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return null;
            }

            @Override
            public BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format,
                    int bucketSize, BucketedSort.ExtraData extra) {
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
}
