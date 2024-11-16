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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
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
            ctx -> loadBlock(ctx, block -> assertThat(block.get(0), equalTo(new BytesRef("foo"))))
        );
    }

    public void testMissing() throws IOException {
        withIndex(source -> {}, ctx -> loadBlock(ctx, block -> assertThat(block.get(0), nullValue())));
    }

    public void testArray() throws IOException {
        withIndex(
            source -> source.startArray("field").value("foo").value("bar").endArray(),
            ctx -> loadBlock(ctx, block -> assertThat(block.get(0), equalTo(List.of(new BytesRef("foo"), new BytesRef("bar")))))
        );
    }

    public void testEmptyArray() throws IOException {
        withIndex(source -> source.startArray("field").endArray(), ctx -> loadBlock(ctx, block -> assertThat(block.get(0), nullValue())));
    }

    private void loadBlock(LeafReaderContext ctx, Consumer<TestBlock> test) throws IOException {
        ValueFetcher valueFetcher = SourceValueFetcher.toString(Set.of("field"));
        BlockSourceReader.LeafIteratorLookup lookup = BlockSourceReader.lookupFromNorms("field");
        BlockLoader loader = new BlockSourceReader.BytesRefsBlockLoader(valueFetcher, lookup);
        assertThat(loader.columnAtATimeReader(ctx), nullValue());
        BlockLoader.RowStrideReader reader = loader.rowStrideReader(ctx);
        assertThat(loader.rowStrideStoredFieldSpec(), equalTo(StoredFieldsSpec.NEEDS_SOURCE));
        BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(loader.rowStrideStoredFieldSpec()).getLoader(ctx, null),
            loader.rowStrideStoredFieldSpec().requiresSource() ? SourceLoader.FROM_STORED_SOURCE.leaf(ctx.reader(), null) : null
        );
        BlockLoader.Builder builder = loader.builder(TestBlock.factory(ctx.reader().numDocs()), 1);
        storedFields.advanceTo(0);
        reader.read(0, storedFields, builder);
        TestBlock block = (TestBlock) builder.build();
        assertThat(block.size(), equalTo(1));
        test.accept(block);
    }

    private void withIndex(CheckedConsumer<XContentBuilder, IOException> buildSource, CheckedConsumer<LeafReaderContext, IOException> test)
        throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text")));
        withLuceneIndex(mapperService, writer -> {
            ParsedDocument parsed = mapperService.documentParser().parseDocument(source(buildSource), mapperService.mappingLookup());
            writer.addDocuments(parsed.docs());
        }, reader -> {
            assertThat(reader.leaves(), hasSize(1));
            test.accept(reader.leaves().get(0));
        });
    }
}
