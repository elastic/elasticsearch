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

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.LuceneTests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.Arrays;

public class SearchSortValuesTests extends AbstractSerializingTestCase<SearchSortValues> {

    public static SearchSortValues createTestItem(XContentType xContentType, boolean transportSerialization) {
        int size = randomIntBetween(1, 20);
        Object[] values = new Object[size];
        if (transportSerialization) {
            DocValueFormat[] sortValueFormats = new DocValueFormat[size];
            for (int i = 0; i < size; i++) {
                Object sortValue = randomSortValue(xContentType, transportSerialization);
                values[i] = sortValue;
                //make sure that for BytesRef, we provide a specific doc value format that overrides format(BytesRef)
                sortValueFormats[i] = sortValue instanceof BytesRef ? DocValueFormat.RAW : randomDocValueFormat();
            }
            return new SearchSortValues(values, sortValueFormats);
        } else {
            //xcontent serialization doesn't write/parse the raw sort values, only the formatted ones
            for (int i = 0; i < size; i++) {
                Object sortValue = randomSortValue(xContentType, transportSerialization);
                //make sure that BytesRef are not provided as formatted values
                sortValue = sortValue instanceof BytesRef ? DocValueFormat.RAW.format((BytesRef)sortValue) : sortValue;
                values[i] = sortValue;
            }
            return new SearchSortValues(values);
        }
    }

    private static Object randomSortValue(XContentType xContentType, boolean transportSerialization) {
        Object randomSortValue = LuceneTests.randomSortValue();
        //to simplify things, we directly serialize what we expect we would parse back when testing xcontent serialization
        return transportSerialization ? randomSortValue : RandomObjects.getExpectedParsedValue(xContentType, randomSortValue);
    }

    private static DocValueFormat randomDocValueFormat() {
        return randomFrom(DocValueFormat.BOOLEAN,
            DocValueFormat.RAW,
            DocValueFormat.IP,
            DocValueFormat.BINARY,
            DocValueFormat.GEOHASH,
            DocValueFormat.GEOTILE);
    }

    @Override
    protected SearchSortValues doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken(); // skip to the elements start array token, fromXContent advances from there if called
        parser.nextToken();
        parser.nextToken();
        SearchSortValues searchSortValues = SearchSortValues.fromXContent(parser);
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
        return SearchSortValues::new;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[]{"sort"};
    }

    public void testToXContent() throws IOException {
        {
            SearchSortValues sortValues = new SearchSortValues(new Object[]{ 1, "foo", 3.0});
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            sortValues.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"sort\":[1,\"foo\",3.0]}", Strings.toString(builder));
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
