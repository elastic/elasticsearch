/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

public class BitmapTermsQueryBuilderTests extends AbstractQueryTestCase<BitmapTermsQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(BitmapPlugin.class);
    }

    @Override
    protected BitmapTermsQueryBuilder doCreateTestQueryBuilder() {
        // Serialize a small single-value bitmap so doToQuery can decode it.
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(randomIntBetween(0, Integer.MAX_VALUE - 1));
        byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
        bitmap.serialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
        return new BitmapTermsQueryBuilder(INT_FIELD_NAME, Base64.getEncoder().encodeToString(bytes));
    }

    @Override
    protected void doAssertLuceneQuery(BitmapTermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertNotNull(query);
    }

    public void testBitmapValueBreakerEstimate() throws IOException {
        // Formula: BASELINE + fieldName.length() * 2 + 64 + value.length() * 2 + 64
        // INT_FIELD_NAME = "mapped_int" (10 chars), "aGk=" (4 chars):
        // 256 + 10*2+64 + 4*2+64 = 256 + 84 + 72 = 412
        String smallValue = "aGk=";
        long limit = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + INT_FIELD_NAME.length() * 2L + 64L + smallValue.length() * 2L
            + 64L;
        // Large: 500-char string → 256 + 84 + 500*2+64 = 1404, exceeds limit 412
        assertParseTimeBreaker(
            limit,
            new BitmapTermsQueryBuilder(INT_FIELD_NAME, smallValue),
            new BitmapTermsQueryBuilder(INT_FIELD_NAME, "x".repeat(500))
        );
    }
}
