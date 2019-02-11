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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;

import java.io.IOException;

public class CategoryQueryContextTests extends QueryContextTestCase<CategoryQueryContext> {

    public static CategoryQueryContext randomCategoryQueryContext() {
        final CategoryQueryContext.Builder builder = CategoryQueryContext.builder();
        builder.setCategory(randomAlphaOfLength(10));
        maybeSet(builder::setBoost, randomIntBetween(1, 10));
        maybeSet(builder::setPrefix, randomBoolean());
        return builder.build();
    }

    @Override
    protected CategoryQueryContext createTestModel() {
        return randomCategoryQueryContext();
    }

    @Override
    protected CategoryQueryContext fromXContent(XContentParser parser) throws IOException {
        return CategoryQueryContext.fromXContent(parser);
    }

    public void testNullCategoryIsIllegal() {
        final CategoryQueryContext categoryQueryContext = randomCategoryQueryContext();
        final CategoryQueryContext.Builder builder = CategoryQueryContext.builder()
            .setBoost(categoryQueryContext.getBoost())
            .setPrefix(categoryQueryContext.isPrefix());
        try {
            builder.build();
            fail("null category is illegal");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "category must not be null");
        }
    }

    public void testIllegalArguments() {
        final CategoryQueryContext.Builder builder = CategoryQueryContext.builder();

        try {
            builder.setCategory(null);
            fail("category must not be null");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "category must not be null");
        }

        try {
            builder.setBoost(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("boost must be positive");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "boost must be greater than 0");
        }
    }
}
