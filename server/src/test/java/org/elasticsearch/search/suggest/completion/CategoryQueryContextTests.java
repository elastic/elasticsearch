/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.xcontent.XContentParser;

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
