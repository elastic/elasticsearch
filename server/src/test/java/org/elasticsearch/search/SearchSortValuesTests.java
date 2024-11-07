/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.LuceneTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;

public class SearchSortValuesTests extends AbstractXContentSerializingTestCase<SearchSortValues> {

    public static SearchSortValues createTestItem(XContentType xContentType, boolean transportSerialization) {
        int size = randomIntBetween(1, 20);
        Object[] values = new Object[size];
        if (transportSerialization) {
            DocValueFormat[] sortValueFormats = new DocValueFormat[size];
            for (int i = 0; i < size; i++) {
                Object sortValue = randomSortValue(xContentType, transportSerialization);
                values[i] = sortValue;
                // make sure that for BytesRef, we provide a specific doc value format that overrides format(BytesRef)
                sortValueFormats[i] = sortValue instanceof BytesRef ? DocValueFormat.RAW : randomDocValueFormat();
            }
            return new SearchSortValues(values, sortValueFormats);
        } else {
            // xcontent serialization doesn't write/parse the raw sort values, only the formatted ones
            for (int i = 0; i < size; i++) {
                Object sortValue = randomSortValue(xContentType, transportSerialization);
                // make sure that BytesRef are not provided as formatted values
                sortValue = sortValue instanceof BytesRef ? DocValueFormat.RAW.format((BytesRef) sortValue) : sortValue;
                values[i] = sortValue;
            }
            return new SearchSortValues(values);
        }
    }

    private static Object randomSortValue(XContentType xContentType, boolean transportSerialization) {
        Object randomSortValue = LuceneTests.randomSortValue();
        // to simplify things, we directly serialize what we expect we would parse back when testing xcontent serialization
        return transportSerialization ? randomSortValue : RandomObjects.getExpectedParsedValue(xContentType, randomSortValue);
    }

    private static DocValueFormat randomDocValueFormat() {
        return randomFrom(
            DocValueFormat.BOOLEAN,
            DocValueFormat.RAW,
            DocValueFormat.IP,
            DocValueFormat.BINARY,
            DocValueFormat.GEOHASH,
            DocValueFormat.GEOTILE
        );
    }

    @Override
    protected SearchSortValues doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken(); // skip to the elements start array token, fromXContent advances from there if called
        parser.nextToken();
        parser.nextToken();
        SearchSortValues searchSortValues = SearchResponseUtils.parseSearchSortValues(parser);
        parser.nextToken();
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.nextToken());
        return searchSortValues;
    }

    @Override
    protected SearchSortValues createXContextTestInstance(XContentType xContentType) {
        return createTestItem(xContentType, false);
    }

    @Override
    protected SearchSortValues createTestInstance() {
        return createTestItem(randomFrom(XContentType.values()), randomBoolean());
    }

    @Override
    protected Writeable.Reader<SearchSortValues> instanceReader() {
        return SearchSortValues::readFrom;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "sort" };
    }

    public void testToXContent() throws IOException {
        {
            SearchSortValues sortValues = new SearchSortValues(new Object[] { 1, "foo", 3.0 });
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            sortValues.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("""
                {"sort":[1,"foo",3.0]}""", Strings.toString(builder));
        }
        {
            SearchSortValues sortValues = new SearchSortValues(new Object[0]);
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            sortValues.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{}", Strings.toString(builder));
        }
    }

    @Override
    protected SearchSortValues mutateInstance(SearchSortValues instance) {
        Object[] sortValues = instance.getFormattedSortValues();
        if (randomBoolean()) {
            return new SearchSortValues(new Object[0]);
        }
        Object[] values = Arrays.copyOf(sortValues, sortValues.length + 1);
        values[sortValues.length] = randomSortValue(randomFrom(XContentType.values()), randomBoolean());
        return new SearchSortValues(values);
    }
}
