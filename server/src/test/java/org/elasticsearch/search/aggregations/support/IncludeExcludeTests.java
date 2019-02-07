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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.OrdinalsFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeSet;

public class IncludeExcludeTests extends ESTestCase {
    public void testEmptyTermsWithOrds() throws IOException {
        IncludeExclude inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());

        inexcl = new IncludeExclude(
                null,
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());
    }

    public void testSingleTermWithOrds() throws IOException {
        SortedSetDocValues ords = new AbstractSortedSetDocValues() {

            boolean consumed = true;

            @Override
            public boolean advanceExact(int docID) {
                consumed = false;
                return true;
            }

            @Override
            public long nextOrd() {
                if (consumed) {
                    return SortedSetDocValues.NO_MORE_ORDS;
                } else {
                    consumed = true;
                    return 0;
                }
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                assertEquals(0, ord);
                return new BytesRef("foo");
            }

            @Override
            public long getValueCount() {
                return 1;
            }

        };
        IncludeExclude inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertTrue(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("bar"))),
                null);
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                null, // means everything included
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));
    }

    public void testPartitionedEquals() throws IOException {
        IncludeExclude serialized = serialize(new IncludeExclude(3, 20), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isRegexBased());
        assertTrue(serialized.isPartitionBased());

        IncludeExclude same = new IncludeExclude(3, 20);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude differentParam1 = new IncludeExclude(4, 20);
        assertFalse(serialized.equals(differentParam1));
        assertTrue(serialized.hashCode() != differentParam1.hashCode());

        IncludeExclude differentParam2 = new IncludeExclude(3, 21);
        assertFalse(serialized.equals(differentParam2));
        assertTrue(serialized.hashCode() != differentParam2.hashCode());
    }

    public void testExactIncludeValuesEquals() throws IOException {
        String[] incValues = { "a", "b" };
        String[] differentIncValues = { "a", "c" };
        IncludeExclude serialized = serialize(new IncludeExclude(incValues, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incValues, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(differentIncValues, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testExactExcludeValuesEquals() throws IOException {
        String[] excValues = { "a", "b" };
        String[] differentExcValues = { "a", "c" };
        IncludeExclude serialized = serialize(new IncludeExclude(null, excValues), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excValues);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, differentExcValues);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexInclude() throws IOException {
        String incRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(incRegex, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(differentRegex, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexExclude() throws IOException {
        String excRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(null, excRegex), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excRegex);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, differentRegex);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    // Serializes/deserializes an IncludeExclude statement with a single clause
    private IncludeExclude serialize(IncludeExclude incExc, ParseField field) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        incExc.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(builder)) {
            XContentParser.Token token = parser.nextToken();
            assertEquals(token, XContentParser.Token.START_OBJECT);
            token = parser.nextToken();
            assertEquals(token, XContentParser.Token.FIELD_NAME);
            assertEquals(field.getPreferredName(), parser.currentName());
            token = parser.nextToken();

            if (field.getPreferredName().equalsIgnoreCase("include")) {
                return IncludeExclude.parseInclude(parser);
            } else if (field.getPreferredName().equalsIgnoreCase("exclude")) {
                return IncludeExclude.parseExclude(parser);
            } else {
                throw new IllegalArgumentException(
                    "Unexpected field name serialized in test: " + field.getPreferredName());
            }
        }
    }

    public void testRegexIncludeAndExclude() throws IOException {
        String incRegex = "foo.*";
        String excRegex = "football";
        String differentExcRegex = "foosball";
        IncludeExclude serialized = serializeMixedRegex(new IncludeExclude(incRegex, excRegex));
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, excRegex);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(incRegex, differentExcRegex);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    // Serializes/deserializes the IncludeExclude statement with include AND
    // exclude clauses
    private IncludeExclude serializeMixedRegex(IncludeExclude incExc) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        incExc.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(builder)) {
            XContentParser.Token token = parser.nextToken();
            assertEquals(token, XContentParser.Token.START_OBJECT);

            IncludeExclude inc = null;
            IncludeExclude exc = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                assertEquals(XContentParser.Token.FIELD_NAME, token);
                if (IncludeExclude.INCLUDE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    token = parser.nextToken();
                    inc = IncludeExclude.parseInclude(parser);
                } else if (IncludeExclude.EXCLUDE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    token = parser.nextToken();
                    exc = IncludeExclude.parseExclude(parser);
                } else {
                    throw new IllegalArgumentException("Unexpected field name serialized in test: " + parser.currentName());
                }
            }
            assertNotNull(inc);
            assertNotNull(exc);
            // Include and Exclude clauses are parsed independently and then merged
            return IncludeExclude.merge(inc, exc);
        }
    }

}
