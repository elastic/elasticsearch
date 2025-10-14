/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class BlockSourceReaderTests extends MapperServiceTestCase {
    public void testSingle() throws IOException {
        withIndex(
            source -> source.field("field", "foo"),
            (mapperService, ctx) -> loadBlock(mapperService, ctx, block -> assertThat(block.get(0), equalTo(new BytesRef("foo"))))
        );
    }

    public void testMissing() throws IOException {
        withIndex(source -> {}, (mapperService, ctx) -> loadBlock(mapperService, ctx, block -> assertThat(block.get(0), nullValue())));
    }

    public void testArray() throws IOException {
        withIndex(
            source -> source.startArray("field").value("foo").value("bar").endArray(),
            (mapperService, ctx) -> loadBlock(
                mapperService,
                ctx,
                block -> assertThat(block.get(0), equalTo(List.of(new BytesRef("foo"), new BytesRef("bar"))))
            )
        );
    }

    public void testEmptyArray() throws IOException {
        withIndex(
            source -> source.startArray("field").endArray(),
            (mapperService, ctx) -> loadBlock(mapperService, ctx, block -> assertThat(block.get(0), nullValue()))
        );
    }

    public void testMoreFields() throws IOException {
        withIndex(
            source -> source.field("field", "foo").field("other_field", "bar").field("other_field_2", 1L),
            (mapperService, ctx) -> loadBlock(mapperService, ctx, block -> assertThat(block.get(0), equalTo(new BytesRef("foo"))))
        );
    }

    private void loadBlock(MapperService mapperService, LeafReaderContext ctx, Consumer<TestBlock> test) throws IOException {
        boolean syntheticSource = mapperService.mappingLookup().isSourceSynthetic();
        var valueFetcher = SourceValueFetcher.toString(Set.of("field"), mapperService.getIndexSettings());
        boolean hasNorms = mapperService.mappingLookup().getFieldType("field").getTextSearchInfo().hasNorms();
        BlockSourceReader.LeafIteratorLookup lookup = hasNorms
            ? BlockSourceReader.lookupFromNorms("field")
            : BlockSourceReader.lookupMatchingAll();
        BlockLoader loader = new BlockSourceReader.BytesRefsBlockLoader(valueFetcher, lookup);
        assertThat(loader.columnAtATimeReader(ctx), nullValue());
        BlockLoader.RowStrideReader reader = loader.rowStrideReader(ctx);
        assertThat(
            loader.rowStrideStoredFieldSpec(),
            equalTo(
                StoredFieldsSpec.withSourcePaths(
                    syntheticSource ? IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE : IgnoredSourceFormat.NO_IGNORED_SOURCE,
                    Set.of("field")
                )
            )
        );
        var sourceLoader = mapperService.mappingLookup()
            .newSourceLoader(new SourceFilter(new String[] { "field" }, null), SourceFieldMetrics.NOOP);
        var sourceLoaderLeaf = sourceLoader.leaf(ctx.reader(), null);

        assertThat(loader.rowStrideStoredFieldSpec().requiresSource(), equalTo(true));
        var storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(loader.rowStrideStoredFieldSpec()).getLoader(ctx, null),
            sourceLoaderLeaf
        );
        BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
        storedFields.advanceTo(0);
        reader.read(0, storedFields, builder);
        TestBlock block = (TestBlock) builder.build();
        assertThat(block.size(), equalTo(1));
        test.accept(block);
    }

    private void withIndex(
        CheckedConsumer<XContentBuilder, IOException> buildSource,
        CheckedBiConsumer<MapperService, LeafReaderContext, IOException> test
    ) throws IOException {
        boolean syntheticSource = randomBoolean();
        String fieldType = randomFrom("text", "keyword");
        MapperService mapperService = syntheticSource
            ? createMapperServiceWithSyntheticSource(mapping(fieldType))
            : createMapperService(mapping(fieldType));
        withLuceneIndex(mapperService, writer -> {
            ParsedDocument parsed = mapperService.documentParser().parseDocument(source(buildSource), mapperService.mappingLookup());
            writer.addDocuments(parsed.docs());
        }, reader -> {
            assertThat(reader.leaves(), hasSize(1));
            test.accept(mapperService, reader.leaves().get(0));
        });
    }

    MapperService createMapperServiceWithSyntheticSource(XContentBuilder mappings) throws IOException {
        var settings = Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            .put("index.mapping.synthetic_source_keep", "arrays")
            .build();
        return createMapperService(getVersion(), settings, () -> true, mappings);
    }

    static XContentBuilder mapping(String type) throws IOException {
        return mapping(b -> {
            b.startObject("field").field("type", type).endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
            b.startObject("other_field_2").field("type", "long").endObject();
        });
    }
}
