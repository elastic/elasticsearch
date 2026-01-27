/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.OrdinalsFilter;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.StringFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class IncludeExcludeTests extends ESTestCase {

    public static IncludeExclude randomIncludeExclude() {
        switch (randomInt(7)) {
            case 0:
                return new IncludeExclude("incl*de", null, null, null);
            case 1:
                return new IncludeExclude("incl*de", "excl*de", null, null);
            case 2:
                return new IncludeExclude("incl*de", null, null, new TreeSet<>(Set.of(newBytesRef("exclude"))));
            case 3:
                return new IncludeExclude(null, "excl*de", null, null);
            case 4:
                return new IncludeExclude(null, "excl*de", new TreeSet<>(Set.of(newBytesRef("include"))), null);
            case 5:
                return new IncludeExclude(null, null, new TreeSet<>(Set.of(newBytesRef("include"))), null);
            case 6:
                return new IncludeExclude(
                    null,
                    null,
                    new TreeSet<>(Set.of(newBytesRef("include"))),
                    new TreeSet<>(Set.of(newBytesRef("exclude")))
                );
            case 7:
                return new IncludeExclude(null, null, null, new TreeSet<>(Set.of(newBytesRef("exclude"))));
            default:
                throw new IllegalArgumentException("got unexpected parameter, expected 0 <= x <= 7");
        }
    }

    public void testEmptyTermsWithOrds() throws IOException {
        IncludeExclude inexcl = new IncludeExclude(null, null, new TreeSet<>(Set.of(new BytesRef("foo"))), null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());

        inexcl = new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());
    }

    private void testCaseTermAccept(IncludeExclude inexcl, boolean accept) throws IOException {
        BytesRef value = new BytesRef("foo");

        SortedSetDocValues ords = new AbstractSortedSetDocValues() {

            boolean consumed = true;

            @Override
            public boolean advanceExact(int docID) {
                consumed = false;
                return true;
            }

            @Override
            public long nextOrd() {
                assert consumed == false;
                consumed = true;
                return 0;
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                assertEquals(0, ord);
                return value;
            }

            @Override
            public long getValueCount() {
                return 1;
            }

        };
        OrdinalsFilter ordFilter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = ordFilter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertEquals(acceptedOrds.get(0), accept);

        StringFilter strFilter = inexcl.convertToStringFilter(DocValueFormat.RAW);
        assertEquals(strFilter.accept(value), accept);
    }

    public void testTermAccept() throws IOException {
        SortedSet<BytesRef> fooSet = new TreeSet<>(Set.of(new BytesRef("foo")));
        SortedSet<BytesRef> barSet = new TreeSet<>(Set.of(new BytesRef("bar")));
        String fooRgx = "f.*";
        String barRgx = "b.*";

        // exclude foo: "foo" is not accepted
        testCaseTermAccept(new IncludeExclude(null, null, null, fooSet), false);
        testCaseTermAccept(new IncludeExclude(null, fooRgx, null, null), false);

        // exclude bar: "foo" is accepted
        testCaseTermAccept(new IncludeExclude(null, null, null, barSet), true);
        testCaseTermAccept(new IncludeExclude(null, barRgx, null, null), true);

        // include foo: "foo" is accepted
        testCaseTermAccept(new IncludeExclude(null, null, fooSet, null), true);
        testCaseTermAccept(new IncludeExclude(fooRgx, null, null, null), true);

        // include bar: "foo" is not accepted
        testCaseTermAccept(new IncludeExclude(null, null, barSet, null), false);
        testCaseTermAccept(new IncludeExclude(barRgx, null, null, null), false);

        // include foo, exclude foo: "foo" is not accepted
        testCaseTermAccept(new IncludeExclude(null, null, fooSet, fooSet), false);
        testCaseTermAccept(new IncludeExclude(null, fooRgx, fooSet, null), false);
        testCaseTermAccept(new IncludeExclude(fooRgx, null, null, fooSet), false);
        testCaseTermAccept(new IncludeExclude(fooRgx, fooRgx, null, null), false);

        // include foo, exclude bar: "foo" is accepted
        testCaseTermAccept(new IncludeExclude(null, null, fooSet, barSet), true);
        testCaseTermAccept(new IncludeExclude(null, barRgx, fooSet, null), true);
        testCaseTermAccept(new IncludeExclude(fooRgx, null, null, barSet), true);
        testCaseTermAccept(new IncludeExclude(fooRgx, barRgx, null, null), true);

        // include bar, exclude foo: "foo" is not accepted
        testCaseTermAccept(new IncludeExclude(null, null, barSet, fooSet), false);
        testCaseTermAccept(new IncludeExclude(null, fooRgx, barSet, null), false);
        testCaseTermAccept(new IncludeExclude(barRgx, null, null, fooSet), false);
        testCaseTermAccept(new IncludeExclude(barRgx, fooRgx, null, null), false);

        // include bar, exclude bar: "foo" is not accepted
        testCaseTermAccept(new IncludeExclude(null, null, barSet, barSet), false);
        testCaseTermAccept(new IncludeExclude(null, barRgx, barSet, null), false);
        testCaseTermAccept(new IncludeExclude(barRgx, null, null, barSet), false);
        testCaseTermAccept(new IncludeExclude(barRgx, barRgx, null, null), false);
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
        SortedSet<BytesRef> incValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("b")));
        SortedSet<BytesRef> differentIncValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("c")));
        IncludeExclude serialized = serialize(new IncludeExclude(null, null, incValues, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, null, incValues, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, null, differentIncValues, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testExactExcludeValuesEquals() throws IOException {
        SortedSet<BytesRef> excValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("b")));
        SortedSet<BytesRef> differentExcValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("c")));
        IncludeExclude serialized = serialize(new IncludeExclude(null, null, null, excValues), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, null, null, excValues);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, null, null, differentExcValues);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexInclude() throws IOException {
        String incRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(incRegex, null, null, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, null, null, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(differentRegex, null, null, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexExclude() throws IOException {
        String excRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(null, excRegex, null, null), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excRegex, null, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, differentRegex, null, null);
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
                throw new IllegalArgumentException("Unexpected field name serialized in test: " + field.getPreferredName());
            }
        }
    }

    public void testRegexIncludeAndExclude() throws IOException {
        String incRegex = "foo.*";
        String excRegex = "football";
        String differentExcRegex = "foosball";
        IncludeExclude serialized = serializeMixedRegex(new IncludeExclude(incRegex, excRegex, null, null));
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, excRegex, null, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(incRegex, differentExcRegex, null, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexIncludeAndSetExclude() throws IOException {
        String incRegex = "foo.*";
        SortedSet<BytesRef> excValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("b")));
        String differentIncRegex = "foosball";
        SortedSet<BytesRef> differentExcValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("c")));

        IncludeExclude serialized = serializeMixedRegex(new IncludeExclude(incRegex, null, null, excValues));
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, null, null, excValues);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude differentInc = new IncludeExclude(differentIncRegex, null, null, excValues);
        assertFalse(serialized.equals(differentInc));
        assertTrue(serialized.hashCode() != differentInc.hashCode());

        IncludeExclude differentExc = new IncludeExclude(incRegex, null, null, differentExcValues);
        assertFalse(serialized.equals(differentExc));
        assertTrue(serialized.hashCode() != differentExc.hashCode());
    }

    public void testSetIncludeAndRegexExclude() throws IOException {
        SortedSet<BytesRef> incValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("b")));
        String excRegex = "foo.*";
        SortedSet<BytesRef> differentIncValues = new TreeSet<>(Set.of(new BytesRef("a"), new BytesRef("c")));
        String differentExcRegex = "foosball";

        IncludeExclude serialized = serializeMixedRegex(new IncludeExclude(null, excRegex, incValues, null));
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excRegex, incValues, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude differentInc = new IncludeExclude(null, excRegex, differentIncValues, null);
        assertFalse(serialized.equals(differentInc));
        assertTrue(serialized.hashCode() != differentInc.hashCode());

        IncludeExclude differentExc = new IncludeExclude(null, differentExcRegex, incValues, null);
        assertFalse(serialized.equals(differentExc));
        assertTrue(serialized.hashCode() != differentExc.hashCode());
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

    public void testInvalidIncludeExcludeCombination() {
        SortedSet<BytesRef> values = new TreeSet<>(Set.of(new BytesRef("foo")));
        String regex = "foo";

        expectThrows(IllegalArgumentException.class, () -> new IncludeExclude((String) null, null, null, null));
        expectThrows(IllegalArgumentException.class, () -> new IncludeExclude(regex, null, values, null));
        expectThrows(IllegalArgumentException.class, () -> new IncludeExclude(null, regex, null, values));
    }

}
