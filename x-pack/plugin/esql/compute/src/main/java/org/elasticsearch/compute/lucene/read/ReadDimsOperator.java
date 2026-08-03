/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Loads dimension fields after a time-series aggregation.
 */
public final class ReadDimsOperator implements Operator {
    public record Factory(ValuesSourceReaderOperator.Factory valuesSourceReader, int docChannel, int tsidChannel)
        implements
            OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ReadDimsOperator(valuesSourceReader.get(driverContext), docChannel, tsidChannel);
        }

        @Override
        public String describe() {
            return "ReadDimsOperator[tsidChannel=" + tsidChannel + ", valuesSourceReader=" + valuesSourceReader.describe() + "]";
        }
    }

    private final Operator valuesReader;
    private final int docChannel;
    private final int tsidChannel;
    private Page prevPage;
    private int[] firstPos = new int[0];
    private int[] ordsArray = new int[0];

    ReadDimsOperator(Operator valuesReader, int docChannel, int tsidChannel) {
        this.valuesReader = valuesReader;
        this.docChannel = docChannel;
        this.tsidChannel = tsidChannel;
    }

    @Override
    public boolean needsInput() {
        return prevPage == null && valuesReader.needsInput();
    }

    @Override
    public void addInput(Page page) {
        assert prevPage == null : "has pending input page";
        prevPage = page;
    }

    @Override
    public void finish() {
        valuesReader.finish();
    }

    @Override
    public boolean isFinished() {
        return prevPage == null && valuesReader.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return prevPage != null || valuesReader.canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public Page getOutput() {
        if (prevPage == null) {
            return valuesReader.getOutput();
        }
        if (prevPage.getPositionCount() == 0) {
            prevPage.close();
            prevPage = null;
            return null;
        }
        Page output = process(prevPage);
        prevPage = null;
        return output;
    }

    Page process(Page page) {
        DocBlock docBlock = page.getBlock(docChannel);
        OrdinalBytesRefVector tsidOrdinals = ordinalVector(page.getBlock(tsidChannel));
        if (tsidOrdinals == null) {
            return page.appendBlocks(readFields(docBlock));
        }
        IntVector ords = tsidOrdinals.getOrdinalsVector();
        int dictSize = tsidOrdinals.getDictionaryVector().getPositionCount();
        final Block[] fields;
        try (var filteredDocs = filterDocBlock(docBlock, dictSize, ords)) {
            fields = readFields(filteredDocs);
        }
        return page.appendBlocks(expand(fields, ords));
    }

    private OrdinalBytesRefVector ordinalVector(BytesRefBlock tsidBlock) {
        BytesRefVector tsidVector = tsidBlock.asVector();
        return tsidVector != null ? tsidVector.asOrdinals() : null;
    }

    private DocBlock filterDocBlock(DocBlock docBlock, int dictSize, IntVector ords) {
        if (dictSize > firstPos.length) {
            firstPos = new int[dictSize];
        }
        Arrays.fill(firstPos, 0, dictSize, -1);
        for (int p = 0; p < ords.getPositionCount(); p++) {
            int ord = ords.getInt(p);
            if (firstPos[ord] == -1) {
                firstPos[ord] = p;
            }
        }
        IntVector shards = null;
        IntVector segments = null;
        IntVector docs = null;
        boolean success = false;
        try {
            DocVector docVector = docBlock.asVector();
            shards = docVector.shards().filter(false, firstPos, 0, dictSize);
            segments = docVector.segments().filter(false, firstPos, 0, dictSize);
            docs = docVector.docs().filter(false, firstPos, 0, dictSize);
            // use noop-shard-refs to avoid expensive incRefs
            var filtered = new DocVector(AlwaysReferencedIndexedByShardId.INSTANCE, shards, segments, docs, DocVector.config()).asBlock();
            success = true;
            return filtered;
        } finally {
            if (success == false) {
                Releasables.close(shards, segments, docs);
            }
        }
    }

    Block[] readFields(DocBlock docBlock) {
        Page readPage = new Page(docBlock.getPositionCount(), docBlock);
        valuesReader.addInput(readPage);
        Page output = valuesReader.getOutput();
        if (output == null) {
            throw new IllegalStateException("ValuesReader returned empty page for docs [" + docBlock + "]");
        }
        // exclude the doc block
        Block[] fields = new Block[output.getBlockCount() - 1];
        for (int i = 1; i < output.getBlockCount(); i++) {
            fields[i - 1] = output.getBlock(i);
        }
        return fields;
    }

    private Block[] expand(Block[] fields, IntVector ords) {
        int positionCount = ords.getPositionCount();
        if (positionCount > ordsArray.length) {
            ordsArray = new int[positionCount];
        }
        ords.copyTo(0, ordsArray, 0, positionCount);
        for (int f = 0; f < fields.length; f++) {
            Block block = fields[f];
            BytesRefVector bytesVector = null;
            if (block instanceof BytesRefBlock bytesBlock) {
                bytesVector = bytesBlock.asVector();
            }
            if (bytesVector != null) {
                ords.incRef();
                fields[f] = new OrdinalBytesRefVector(ords, bytesVector).asBlock();
            } else {
                fields[f] = block.filter(true, ordsArray, 0, positionCount);
                block.close();
            }
        }
        return fields;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(valuesReader, prevPage);
    }

    @Override
    public Status status() {
        return valuesReader.status();
    }

    @Override
    public String toString() {
        return "ReadDimsOperator[tsidChannel=" + tsidChannel + ", valuesSourceReader=" + valuesReader + "]";
    }
}
