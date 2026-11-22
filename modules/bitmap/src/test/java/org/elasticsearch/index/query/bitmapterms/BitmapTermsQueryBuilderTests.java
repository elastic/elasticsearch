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
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Covers how {@link BitmapTermsQueryBuilder} resolves its field: a field alias must query the field it
 * points at, and a field the index does not map must match nothing rather than fail the shard, so that a
 * search across indices that map the field differently still returns the hits of those that do.
 */
public class BitmapTermsQueryBuilderTests extends AbstractQueryTestCase<BitmapTermsQueryBuilder> {

    private static final String LONG_FIELD_NAME = "mapped_long";
    private static final String LONG_ALIAS_FIELD_NAME = "mapped_long_alias";
    private static final String INT_TERMS_FIELD_NAME = "mapped_int_terms";
    private static final String INT_TERMS_ALIAS_FIELD_NAME = "mapped_int_terms_alias";
    private static final String LONG_TERMS_FIELD_NAME = "mapped_long_terms";
    private static final String UNMAPPED_FIELD_NAME = "unmapped_field";

    private static final Map<String, String> ALIAS_TARGETS = Map.of(
        INT_ALIAS_FIELD_NAME,
        INT_FIELD_NAME,
        INT_TERMS_ALIAS_FIELD_NAME,
        INT_TERMS_FIELD_NAME,
        LONG_ALIAS_FIELD_NAME,
        LONG_FIELD_NAME
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(BitmapPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.toString(
                    PutMappingRequest.simpleMapping(
                        LONG_FIELD_NAME,
                        "type=long",
                        LONG_ALIAS_FIELD_NAME,
                        "type=alias,path=" + LONG_FIELD_NAME,
                        INT_TERMS_FIELD_NAME,
                        "type=integer,index_terms=true",
                        INT_TERMS_ALIAS_FIELD_NAME,
                        "type=alias,path=" + INT_TERMS_FIELD_NAME,
                        LONG_TERMS_FIELD_NAME,
                        "type=long,index_terms=true"
                    )
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected BitmapTermsQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(
            INT_FIELD_NAME,
            INT_ALIAS_FIELD_NAME,
            INT_TERMS_FIELD_NAME,
            INT_TERMS_ALIAS_FIELD_NAME,
            LONG_FIELD_NAME,
            LONG_ALIAS_FIELD_NAME,
            LONG_TERMS_FIELD_NAME,
            UNMAPPED_FIELD_NAME
        );
        String value = switch (concreteFieldName(fieldName)) {
            case INT_FIELD_NAME, INT_TERMS_FIELD_NAME, UNMAPPED_FIELD_NAME -> intBitmap(
                IntStream.generate(() -> randomIntBetween(0, Integer.MAX_VALUE)).limit(randomIntBetween(0, 20)).toArray()
            );
            case LONG_FIELD_NAME, LONG_TERMS_FIELD_NAME -> longBitmap(
                LongStream.generate(() -> randomNonNegativeLong()).limit(randomIntBetween(0, 20)).toArray()
            );
            default -> throw new AssertionError("unexpected field [" + fieldName + "]");
        };
        return new BitmapTermsQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(BitmapTermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        if (queryBuilder.fieldName().equals(UNMAPPED_FIELD_NAME)) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            return;
        }
        String concreteFieldName = concreteFieldName(queryBuilder.fieldName());
        byte[] bytes = Base64.getDecoder().decode(queryBuilder.value());
        Query expected = switch (concreteFieldName) {
            case INT_FIELD_NAME -> new BitmapBKDQuery(concreteFieldName, IntBitmap.deserialize(bytes));
            case LONG_FIELD_NAME -> new BitmapBKDQuery(concreteFieldName, LongBitmap.deserializePortable(bytes));
            case INT_TERMS_FIELD_NAME -> new BitmapTermsQuery(concreteFieldName, IntBitmap.deserialize(bytes));
            case LONG_TERMS_FIELD_NAME -> new BitmapTermsQuery(concreteFieldName, LongBitmap.deserializePortable(bytes));
            default -> throw new AssertionError("unexpected field [" + concreteFieldName + "]");
        };
        assertThat(query, equalTo(expected));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        BitmapTermsQueryBuilder queryBuilder = new BitmapTermsQueryBuilder(UNMAPPED_FIELD_NAME, intBitmap(1, 2, 3));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testUnmappedFieldRewritesToMatchNone() throws IOException {
        BitmapTermsQueryBuilder query = new BitmapTermsQueryBuilder(UNMAPPED_FIELD_NAME, intBitmap(1, 2, 3));
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            assertThat(query.rewrite(context), instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testMappedFieldIsNotRewritten() throws IOException {
        BitmapTermsQueryBuilder query = new BitmapTermsQueryBuilder(INT_ALIAS_FIELD_NAME, intBitmap(1, 2, 3));
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            assertThat(query.rewrite(context), sameInstance(query));
        }
    }

    public void testAliasQueriesTargetField() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        String intValue = intBitmap(1, 2, 3);
        String longValue = longBitmap(1L, 1L << 32, Long.MAX_VALUE);
        assertThat(
            new BitmapTermsQueryBuilder(INT_ALIAS_FIELD_NAME, intValue).toQuery(context),
            equalTo(new BitmapTermsQueryBuilder(INT_FIELD_NAME, intValue).toQuery(context))
        );
        assertThat(
            new BitmapTermsQueryBuilder(INT_TERMS_ALIAS_FIELD_NAME, intValue).toQuery(context),
            equalTo(new BitmapTermsQueryBuilder(INT_TERMS_FIELD_NAME, intValue).toQuery(context))
        );
        assertThat(
            new BitmapTermsQueryBuilder(LONG_ALIAS_FIELD_NAME, longValue).toQuery(context),
            equalTo(new BitmapTermsQueryBuilder(LONG_FIELD_NAME, longValue).toQuery(context))
        );
    }

    private static String concreteFieldName(String fieldName) {
        return ALIAS_TARGETS.getOrDefault(fieldName, fieldName);
    }

    private static String intBitmap(int... values) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RoaringBitmap.bitmapOf(values).serialize(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static String longBitmap(long... values) {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        bitmap.add(values);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serializePortable(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
}
