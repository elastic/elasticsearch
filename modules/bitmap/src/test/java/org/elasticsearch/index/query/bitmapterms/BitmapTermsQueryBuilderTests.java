/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
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
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Builds queries on an integer field, its alias, and an unmapped field: the alias must query the field it
 * points at, and the unmapped field must match nothing.
 */
public class BitmapTermsQueryBuilderTests extends AbstractQueryTestCase<BitmapTermsQueryBuilder> {

    private static final String UNMAPPED_FIELD_NAME = "unmapped_field";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(BitmapPlugin.class);
    }

    @Override
    protected BitmapTermsQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(INT_FIELD_NAME, INT_ALIAS_FIELD_NAME, UNMAPPED_FIELD_NAME);
        int[] values = IntStream.generate(() -> randomNonNegativeInt()).limit(randomIntBetween(0, 20)).toArray();
        return new BitmapTermsQueryBuilder(fieldName, bitmap(values));
    }

    @Override
    protected void doAssertLuceneQuery(BitmapTermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        if (queryBuilder.fieldName().equals(UNMAPPED_FIELD_NAME)) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            IntBitmap values = IntBitmap.deserialize(Base64.getDecoder().decode(queryBuilder.value()));
            assertThat(query, equalTo(new BitmapBKDQuery(expectedFieldName(queryBuilder.fieldName()), values)));
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        BitmapTermsQueryBuilder queryBuilder = new BitmapTermsQueryBuilder(UNMAPPED_FIELD_NAME, bitmap(1));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
        assertThat(queryBuilder.rewrite(createQueryRewriteContext()), instanceOf(MatchNoneQueryBuilder.class));
    }

    private static String bitmap(int... values) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(values);
        ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
        bitmap.serialize(buffer);
        return Base64.getEncoder().encodeToString(buffer.array());
    }
}
