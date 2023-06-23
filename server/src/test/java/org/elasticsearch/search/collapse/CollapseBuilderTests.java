/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.collapse;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitBuilderTests;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollapseBuilderTests extends AbstractXContentSerializingTestCase<CollapseBuilder> {
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    public static CollapseBuilder randomCollapseBuilder() {
        return randomCollapseBuilder(true);
    }

    public static CollapseBuilder randomCollapseBuilder(boolean multiInnerHits) {
        CollapseBuilder builder = new CollapseBuilder(randomAlphaOfLength(10));
        builder.setMaxConcurrentGroupRequests(randomIntBetween(1, 48));
        int numInnerHits = randomIntBetween(0, multiInnerHits ? 5 : 1);
        if (numInnerHits == 1) {
            InnerHitBuilder innerHit = InnerHitBuilderTests.randomInnerHits();
            builder.setInnerHits(innerHit);
        } else if (numInnerHits > 1) {
            List<InnerHitBuilder> innerHits = new ArrayList<>(numInnerHits);
            for (int i = 0; i < numInnerHits; i++) {
                innerHits.add(InnerHitBuilderTests.randomInnerHits());
            }

            builder.setInnerHits(innerHits);
        }

        return builder;
    }

    @Override
    protected CollapseBuilder createTestInstance() {
        return randomCollapseBuilder();
    }

    @Override
    protected Writeable.Reader<CollapseBuilder> instanceReader() {
        return CollapseBuilder::new;
    }

    @Override
    protected CollapseBuilder mutateInstance(CollapseBuilder instance) throws IOException {
        CollapseBuilder newBuilder;
        switch (between(0, 2)) {
            case 0 -> {
                newBuilder = new CollapseBuilder(instance.getField() + randomAlphaOfLength(10));
                newBuilder.setMaxConcurrentGroupRequests(instance.getMaxConcurrentGroupRequests());
                newBuilder.setInnerHits(instance.getInnerHits());
            }
            case 1 -> {
                newBuilder = copyInstance(instance);
                newBuilder.setMaxConcurrentGroupRequests(instance.getMaxConcurrentGroupRequests() + between(1, 20));
            }
            default -> {
                newBuilder = copyInstance(instance);
                List<InnerHitBuilder> innerHits = new ArrayList<>(newBuilder.getInnerHits());
                for (int i = 0; i < between(1, 5); i++) {
                    innerHits.add(InnerHitBuilderTests.randomInnerHits());
                }
                newBuilder.setInnerHits(innerHits);
            }
        }
        return newBuilder;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public void testBuild() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())))) {
            writer.commit();
        }
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        try (IndexReader reader = DirectoryReader.open(dir)) {
            when(searchExecutionContext.getIndexReader()).thenReturn(reader);

            MappedFieldType numberFieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);
            when(searchExecutionContext.getFieldType("field")).thenReturn(numberFieldType);
            CollapseBuilder builder = new CollapseBuilder("field");
            CollapseContext collapseContext = builder.build(searchExecutionContext);
            assertEquals(collapseContext.getFieldType(), numberFieldType);

            numberFieldType = new NumberFieldMapper.NumberFieldType(
                "field",
                NumberFieldMapper.NumberType.LONG,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null
            );
            when(searchExecutionContext.getFieldType("field")).thenReturn(numberFieldType);
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> builder.build(searchExecutionContext));
            assertEquals(exc.getMessage(), "cannot collapse on field `field` without `doc_values`");

            numberFieldType = new NumberFieldMapper.NumberFieldType(
                "field",
                NumberFieldMapper.NumberType.LONG,
                false,
                false,
                true,
                false,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null
            );
            when(searchExecutionContext.getFieldType("field")).thenReturn(numberFieldType);
            builder.setInnerHits(new InnerHitBuilder());
            exc = expectThrows(IllegalArgumentException.class, () -> builder.build(searchExecutionContext));
            assertEquals(
                exc.getMessage(),
                "cannot expand `inner_hits` for collapse field `field`, only indexed field can retrieve `inner_hits`"
            );

            MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType("field");
            when(searchExecutionContext.getFieldType("field")).thenReturn(keywordFieldType);
            CollapseBuilder kbuilder = new CollapseBuilder("field");
            collapseContext = kbuilder.build(searchExecutionContext);
            assertEquals(collapseContext.getFieldType(), keywordFieldType);

            keywordFieldType = new KeywordFieldMapper.KeywordFieldType("field", true, false, Collections.emptyMap());
            when(searchExecutionContext.getFieldType("field")).thenReturn(keywordFieldType);
            exc = expectThrows(IllegalArgumentException.class, () -> kbuilder.build(searchExecutionContext));
            assertEquals(exc.getMessage(), "cannot collapse on field `field` without `doc_values`");

            keywordFieldType = new KeywordFieldMapper.KeywordFieldType("field", false, true, Collections.emptyMap());
            when(searchExecutionContext.getFieldType("field")).thenReturn(keywordFieldType);
            kbuilder.setInnerHits(new InnerHitBuilder());
            exc = expectThrows(IllegalArgumentException.class, () -> builder.build(searchExecutionContext));
            assertEquals(
                exc.getMessage(),
                "cannot expand `inner_hits` for collapse field `field`, only indexed field can retrieve `inner_hits`"
            );

        }
    }

    public void testBuildWithExceptions() {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        {
            CollapseBuilder builder = new CollapseBuilder("unknown_field");
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> builder.build(searchExecutionContext));
            assertEquals(exc.getMessage(), "no mapping found for `unknown_field` in order to collapse on");
        }

        {
            MappedFieldType fieldType = new MappedFieldType("field", true, false, true, TextSearchInfo.NONE, Collections.emptyMap()) {
                @Override
                public String typeName() {
                    return "some_type";
                }

                @Override
                public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Query termQuery(Object value, SearchExecutionContext context) {
                    return null;
                }

                public Query existsQuery(SearchExecutionContext context) {
                    return null;
                }
            };
            when(searchExecutionContext.getFieldType("field")).thenReturn(fieldType);
            CollapseBuilder builder = new CollapseBuilder("field");
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> builder.build(searchExecutionContext));
            assertEquals(exc.getMessage(), "collapse is not supported for the field [field] of the type [some_type]");
        }
    }

    @Override
    protected CollapseBuilder doParseInstance(XContentParser parser) {
        return CollapseBuilder.fromXContent(parser);
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        // disable xcontent shuffling on the highlight builder
        return new String[] { "fields" };
    }
}
