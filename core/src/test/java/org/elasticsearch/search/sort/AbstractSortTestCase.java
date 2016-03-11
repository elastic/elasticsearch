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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractSortTestCase<T extends SortBuilder & NamedWriteable<T> & SortElementParserTemp<T>> extends ESTestCase {

    protected static NamedWriteableRegistry namedWriteableRegistry;

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    static IndicesQueriesRegistry indicesQueriesRegistry;

    @BeforeClass
    public static void init() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(SortBuilder.class, GeoDistanceSortBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SortBuilder.class, ScoreSortBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SortBuilder.class, ScriptSortBuilder.PROTOTYPE);
        indicesQueriesRegistry = new SearchModule(Settings.EMPTY, namedWriteableRegistry).buildQueryParserRegistry();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /** Returns random sort that is put under test */
    protected abstract T createTestItem();

    /** Returns mutated version of original so the returned sort is different in terms of equals/hashcode */
    protected abstract T mutate(T original) throws IOException;

    /**
     * Test that creates new sort from a random test sort and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testItem = createTestItem();

            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            builder.startObject();
            testItem.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            XContentParser itemParser = XContentHelper.createParser(builder.bytes());
            itemParser.nextToken();

            /*
             * filter out name of sort, or field name to sort on for element fieldSort
             */
            itemParser.nextToken();
            String elementName = itemParser.currentName();
            itemParser.nextToken();

            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
            context.reset(itemParser);
            SortBuilder parsedItem = testItem.fromXContent(context, elementName);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    /**
     * Test serialization and deserialization of the test sort.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testsort = createTestItem();
            T deserializedsort = copyItem(testsort);
            assertEquals(testsort, deserializedsort);
            assertEquals(testsort.hashCode(), deserializedsort.hashCode());
            assertNotSame(testsort, deserializedsort);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T firstsort = createTestItem();
            assertFalse("sort is equal to null", firstsort.equals(null));
            assertFalse("sort is equal to incompatible type", firstsort.equals(""));
            assertTrue("sort is not equal to self", firstsort.equals(firstsort));
            assertThat("same sort's hashcode returns different values if called multiple times", firstsort.hashCode(),
                    equalTo(firstsort.hashCode()));
            assertThat("different sorts should not be equal", mutate(firstsort), not(equalTo(firstsort)));
            assertThat("different sorts should have different hashcode", mutate(firstsort).hashCode(), not(equalTo(firstsort.hashCode())));

            T secondsort = copyItem(firstsort);
            assertTrue("sort is not equal to self", secondsort.equals(secondsort));
            assertTrue("sort is not equal to its copy", firstsort.equals(secondsort));
            assertTrue("equals is not symmetric", secondsort.equals(firstsort));
            assertThat("sort copy's hashcode is different from original hashcode", secondsort.hashCode(), equalTo(firstsort.hashCode()));

            T thirdsort = copyItem(secondsort);
            assertTrue("sort is not equal to self", thirdsort.equals(thirdsort));
            assertTrue("sort is not equal to its copy", secondsort.equals(thirdsort));
            assertThat("sort copy's hashcode is different from original hashcode", secondsort.hashCode(), equalTo(thirdsort.hashCode()));
            assertTrue("equals is not transitive", firstsort.equals(thirdsort));
            assertThat("sort copy's hashcode is different from original hashcode", firstsort.hashCode(), equalTo(thirdsort.hashCode()));
            assertTrue("equals is not symmetric", thirdsort.equals(secondsort));
            assertTrue("equals is not symmetric", thirdsort.equals(firstsort));
        }
    }

    @SuppressWarnings("unchecked")
    protected T copyItem(T original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                T prototype = (T) namedWriteableRegistry.getPrototype(SortBuilder.class,
                        original.getWriteableName());
                return prototype.readFrom(in);
            }
        }
    }
}
