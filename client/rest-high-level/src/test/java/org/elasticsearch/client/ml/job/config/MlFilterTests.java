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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.contains;

public class MlFilterTests extends AbstractXContentTestCase<MlFilter> {

    public static MlFilter createTestFilter() {
        return new MlFilterTests().createTestInstance();
    }

    @Override
    protected MlFilter createTestInstance() {
        return createRandom();
    }

    public static MlFilter createRandom() {
        return createRandomBuilder(randomAlphaOfLength(10)).build();
    }

    public static MlFilter.Builder createRandomBuilder(String filterId) {
        String description = null;
        if (randomBoolean()) {
            description = randomAlphaOfLength(20);
        }

        int size = randomInt(10);
        SortedSet<String> items = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        return MlFilter.builder(filterId).setDescription(description).setItems(items);
    }

    @Override
    protected MlFilter doParseInstance(XContentParser parser) {
        return MlFilter.PARSER.apply(parser, null).build();
    }

    public void testNullId() {
        expectThrows(NullPointerException.class, () -> MlFilter.builder(null).build());
    }

    public void testNullItems() {
        expectThrows(NullPointerException.class,
            () -> MlFilter.builder(randomAlphaOfLength(10)).setItems((SortedSet<String>) null).build());
    }

    public void testItemsAreSorted() {
        MlFilter filter = MlFilter.builder("foo").setItems("c", "b", "a").build();
        assertThat(filter.getItems(), contains("a", "b", "c"));
    }

    public void testGetItemsReturnsUnmodifiable() {
        MlFilter filter = MlFilter.builder("foo").setItems("c", "b", "a").build();
        expectThrows(UnsupportedOperationException.class, () -> filter.getItems().add("x"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
