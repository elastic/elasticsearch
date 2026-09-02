/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.junit.After;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReadDimsOperatorTests extends ComputeTestCase {

    private final Directory directory = newDirectory();
    private DirectoryReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testUseOrdinals() throws Exception {
        String[] dimValues = { "dim_a", "dim_a", "dim_b", "dim_b", "dim_c", "dim_c" };

        try (IndexWriter w = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (String val : dimValues) {
                Document doc = new Document();
                doc.add(new SortedDocValuesField("dim", new BytesRef(val)));
                w.addDocument(doc);
            }
            w.commit();
        }
        reader = DirectoryReader.open(directory);

        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("dim");
        ValuesSourceReaderOperator.Factory readerFactory = new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            List.of(
                new ValuesSourceReaderOperator.FieldInfo(
                    "dim",
                    ElementType.BYTES_REF,
                    false,
                    (ctx, shardIdx) -> ValuesSourceReaderOperator.load(ft.blockLoader(ValuesSourceReaderOperatorTests.blContext()))
                )
            ),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    sourcePaths -> SourceLoader.FROM_STORED_SOURCE,
                    ValuesSourceReaderOperatorTests.STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            randomBoolean(),
            0,
            randomDoubleBetween(0.1, 10.0, true),
            500,
            () -> 0L
        );

        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        DocBlock docBlock;
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(blockFactory, 6)) {
            for (int i = 0; i < 6; i++) {
                builder.append(0, 0, i);
            }
            docBlock = builder.build(DocVector.config()).asBlock();
        }
        BytesRefBlock ordinalTsidBlock;
        try (var dictBuilder = blockFactory.newBytesRefVectorBuilder(3); var ordsBuilder = blockFactory.newIntVectorFixedBuilder(6)) {
            dictBuilder.appendBytesRef(new BytesRef("tsid_a"));
            dictBuilder.appendBytesRef(new BytesRef("tsid_b"));
            dictBuilder.appendBytesRef(new BytesRef("tsid_c"));
            ordsBuilder.appendInt(0).appendInt(0).appendInt(1).appendInt(1).appendInt(2).appendInt(2);
            IntVector ords = ordsBuilder.build();
            ordinalTsidBlock = new OrdinalBytesRefVector(ords, dictBuilder.build()).asBlock();
        }
        BytesRefBlock plainTsidBlock;
        try (var builder = blockFactory.newBytesRefBlockBuilder(6)) {
            String[] tsids = { "tsid_a", "tsid_a", "tsid_b", "tsid_b", "tsid_c", "tsid_c" };
            for (String tsid : tsids) {
                builder.appendBytesRef(new BytesRef(tsid));
            }
            plainTsidBlock = builder.build();
        }
        docBlock.incRef(); // retain for second run
        Page ordinalOut = runReadDims(readerFactory, driverContext, new Page(docBlock, ordinalTsidBlock));
        Page plainOut = runReadDims(readerFactory, driverContext, new Page(docBlock, plainTsidBlock));

        assertThat(ordinalOut.getPositionCount(), equalTo(6));
        assertThat(plainOut.getPositionCount(), equalTo(6));

        BytesRef scratch = new BytesRef();
        for (int p = 0; p < 6; p++) {
            BytesRef ordVal = ((BytesRefBlock) ordinalOut.getBlock(2)).getBytesRef(p, scratch);
            BytesRef plainVal = ((BytesRefBlock) plainOut.getBlock(2)).getBytesRef(p, scratch);
            assertThat("dim at position " + p + " (ordinal path)", ordVal, equalTo(new BytesRef(dimValues[p])));
            assertThat("dim at position " + p + " (paths match)", ordVal, equalTo(plainVal));
        }
        Releasables.close(ordinalOut, plainOut);
    }

    private static Page runReadDims(ValuesSourceReaderOperator.Factory readerFactory, DriverContext driverContext, Page input) {
        try (Operator operator = new ReadDimsOperator.Factory(readerFactory, 0, 1).get(driverContext)) {
            operator.addInput(input);
            Page out = operator.getOutput();
            assertNotNull(out);
            return out;
        }
    }
}
