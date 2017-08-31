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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;

public class NestedSortBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            NestedSortBuilder testItem = createRandomNestedSort(3);
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            testItem.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);
            XContentParser parser = createParser(shuffled);
            parser.nextToken();
            NestedSortBuilder parsedItem = NestedSortBuilder.fromXContent(parser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    /**
     * Create a {@link NestedSortBuilder} with random path and filter of the given depth.
     */
    public static NestedSortBuilder createRandomNestedSort(int depth) {
        NestedSortBuilder nestedSort = new NestedSortBuilder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            nestedSort.setFilter(AbstractSortTestCase.randomNestedFilter());
        }
        if (depth > 0) {
            nestedSort.setNestedSort(createRandomNestedSort(depth - 1));
        }
        return nestedSort;
    }

    /**
     * Test serialization of the test nested sort.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            NestedSortBuilder testsort = createRandomNestedSort(3);
            NestedSortBuilder deserializedsort = copy(testsort);
            assertEquals(testsort, deserializedsort);
            assertEquals(testsort.hashCode(), deserializedsort.hashCode());
            assertNotSame(testsort, deserializedsort);
        }
    }

    private static NestedSortBuilder copy(NestedSortBuilder nestedSort) throws IOException {
        return copyWriteable(nestedSort, namedWriteableRegistry, NestedSortBuilder::new);
    }

    private static NestedSortBuilder mutate(NestedSortBuilder original) throws IOException {
        NestedSortBuilder mutated = original.getNestedSort();
        int parameter = randomIntBetween(0, 2);
        switch (parameter) {
        case 0:
            mutated = new NestedSortBuilder(original.getPath()+"_suffix");
            mutated.setFilter(original.getFilter());
            mutated.setNestedSort(original.getNestedSort());
            break;
        case 1:
            mutated.setFilter(randomValueOtherThan(original.getFilter(), AbstractSortTestCase::randomNestedFilter));
            break;
        case 2:
        default:
            mutated.setNestedSort(randomValueOtherThan(original.getNestedSort(), () -> NestedSortBuilderTests.createRandomNestedSort(3)));
            break;
        }
        return mutated;
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomNestedSort(3), NestedSortBuilderTests::copy,
                    NestedSortBuilderTests::mutate);
        }
    }
}
