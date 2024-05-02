/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.compute.operator.HashLookupOperator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

// public class DedupOperator extends HashLookupOperator {
//     public DedupOperator(BlockFactory blockFactory, HashLookupOperator.Key[] keys, int[] blockMapping) {
//         super(blockFactory, keys, blockMapping);
//     }
// }

public class DedupOperator implements Operator {
    private final int[] fields;

    private Page lastInput;

    private boolean finished;

    private HashSet<Integer> seenKeys = new HashSet<Integer>();

    /**
     * Count of pages that have been processed by this operator.
     */
    private int pagesIn;
    private int pagesOut;

    public DedupOperator(List<Integer> fields) {
        this.fields = fields.stream().mapToInt(Integer::intValue).toArray();;        
    }

    public record DedupOperatorFactory(List<Integer> fields) implements OperatorFactory {

        @Override
        public DedupOperator get(DriverContext driverContext) {
            System.out.println(describe());
            return new DedupOperator(fields);
        }

        @Override
        public String describe() {
            if (fields.size() < 10) {
                return "DedupOperator[fields = " + fields + "]";
            }
            return "DedupOperator[fields = [size: " + fields.size() + "]]";
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastInput == null;
    }

    @Override
    public void addInput(Page page) {
        assert lastInput == null : "has pending input page";
        lastInput = page;
        // this.expandingBlock = prev.getBlock(channel);
        pagesIn++;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && lastInput == null;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }

        pagesOut++;
        Page result;

        // positions -> rows
        // valueCount -> number of values in given field
        // 

        // Go over all rows and check if the row is a duplicate
        for (int p = 0; p < lastInput.getPositionCount(); p++) {
            
            
            // if (test.isNull(p) || test.getValueCount(p) != 1) {
            //     // Null is like false
            //     // And, for now, multivalued results are like false too
            //     continue;
            // }
            // if (test.getBoolean(test.getFirstValueIndex(p))) {
            //     positions[rowCount++] = p;
            // }
        }

        // if (rowCount == 0) {
        //     page.releaseBlocks();
        //     return null;
        // }
        // if (rowCount == page.getPositionCount()) {
        //     return page;
        // }
        // positions = Arrays.copyOf(positions, rowCount);

        // Block[] filteredBlocks = new Block[page.getBlockCount()];
        // boolean success = false;
        // try {
        //     for (int i = 0; i < page.getBlockCount(); i++) {
        //         filteredBlocks[i] = page.getBlock(i).filter(positions);
        //     }
        //     success = true;
        // } finally {
        //     page.releaseBlocks();
        //     if (success == false) {
        //         Releasables.closeExpectNoException(filteredBlocks);
        //     }
        // }
        // return new Page(filteredBlocks);

        // lastInput.getPositionCount() - number of rows
        // int[] filter = new int[limitRemaining];
        // for (int i = 0; i < limitRemaining; i++) {
        //     filter[i] = i;
        // }
        
        Block[] blocks = new Block[lastInput.getBlockCount()];
        boolean success = false;
        try {
            for (int b = 0; b < blocks.length; b++) {
                System.out.println(lastInput.getBlock(b).getFirstValueIndex(fields[0]));
                System.out.println(lastInput.getBlock(b).getClass());
                blocks[b] = lastInput.getBlock(b).filter(fields);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(lastInput::releaseBlocks, Releasables.wrap(blocks));
            } else {
                lastInput.releaseBlocks();
            }
            lastInput = null;
        }
        result = new Page(blocks);
        lastInput = null;
        
        return result;
    }

    @Override
    public Status status() {
        return new Status(pagesIn);
    }

    @Override
    public void close() {
        if (lastInput != null) {
            lastInput.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        if (fields.length < 10) {
            return "DedupOperator[fields = " + Arrays.toString(fields) + "]";
        }
        return "DedupOperator[fields = [size: " + fields.length + "]]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "dedup",
            Status::new
        );

        /**
         * Count of pages that have been processed by this operator.
         */
        private final int pagesProcessed;

        protected Status(int pagesProcessed) {
            this.pagesProcessed = pagesProcessed;
        }

        protected Status(StreamInput in) throws IOException {
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Count of pages that have been processed by this operator.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }
    }
}
