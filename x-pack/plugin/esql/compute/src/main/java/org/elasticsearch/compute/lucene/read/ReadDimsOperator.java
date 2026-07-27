/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

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

    ReadDimsOperator(Operator valuesReader, int ddocChannel, int tsidChannel) {
        this.valuesReader = valuesReader;
        this.docChannel = ddocChannel;
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
        Block[] fields = readFields(docBlock);
        return page.appendBlocks(fields);
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
