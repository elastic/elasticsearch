/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;

public class UnpivotOperator extends AbstractPageMappingOperator {
    private static final Logger logger = LogManager.getLogger(UnpivotOperator.class);
    private final List<String> names;

    public record Factory(int[] sourceChannels, List<String> names) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new UnpivotOperator(sourceChannels, names);
        }

        @Override
        public String describe() {
            return "UnpivotOperator[surceChannels=" + sourceChannels + "]";
        }
    }

    private final int[] sourceChannels;

    public UnpivotOperator(int[] sourceChannels, List<String> names) {
        this.sourceChannels = sourceChannels;
        this.names = names;
    }

    @Override
    protected Page process(Page page) {
        try {
            int inputRows = page.getPositionCount();
            int[] duplicateFilter = new int[inputRows * sourceChannels.length];
            int counter = 0;
            for (int i = 0; i < inputRows; i++) {
                for (int j = 0; j < sourceChannels.length; j++) {
                    duplicateFilter[counter++] = i;
                }
            }

            Block[] result = new Block[page.getBlockCount() - sourceChannels.length + 2];
            int nextBlock = 0;
            for (int i = 0; i < page.getBlockCount(); i++) {
                if (contains(sourceChannels, i) == false) {
                    result[nextBlock++] = page.getBlock(i).filter(duplicateFilter);
                }
            }

            result[result.length - 2] = blockForKey(page);
            result[result.length - 1] = blockForValue(page);

            // TODO review memory management

            return new Page(result);
        } finally {
            page.releaseBlocks();
        }

    }

    private BytesRefBlock blockForKey(Page page) {
        BytesRefBlock.Builder result = page.getBlock(0)
            .blockFactory()
            .newBytesRefBlockBuilder(page.getPositionCount() * sourceChannels.length);
        BytesRef[] cols = names.stream().map(x -> new BytesRef(x)).toList().toArray(new BytesRef[names.size()]);

        for (int i = 0; i < page.getPositionCount(); i++) {
            for (int j = 0; j < cols.length; j++) {
                result.appendBytesRef(cols[j]);
            }
        }
        return result.build();
    }

    private Block blockForValue(Page page) {
        ElementType type = page.getBlock(sourceChannels[0]).elementType();
        for (int i = 0; i < sourceChannels.length; i++) {
            Block result = switch (type) {
                case BOOLEAN -> blockForBoolean(page);
                case INT -> blockForInt(page);
                case LONG -> blockForLong(page);
                case FLOAT -> blockForFloat(page);
                case DOUBLE -> blockForDouble(page);
                case NULL -> null;
                case BYTES_REF -> blockForBytesRef(page);
                case DOC -> blockForDoc(page);
                case COMPOSITE -> throw new IllegalArgumentException();
                case UNKNOWN -> throw new IllegalArgumentException();
            };
            if (result == null) {
                continue;
            }
            return result;
        }
        return page.getBlock(0).blockFactory().newConstantNullBlock(page.getPositionCount() * sourceChannels.length);

    }

    private Block blockForDoc(Page page) {
        throw new UnsupportedOperationException(); // TODO
    }

    private Block blockForBytesRef(Page page) {
        BytesRefBlock.Builder result = page.getBlock(0)
            .blockFactory()
            .newBytesRefBlockBuilder(page.getPositionCount() * sourceChannels.length);

        BytesRefBlock[] inputBlocks = new BytesRefBlock[sourceChannels.length];
        for (int i = 0; i < sourceChannels.length; i++) {
            inputBlocks[i] = page.getBlock(sourceChannels[i]);
        }
        BytesRef spare = new BytesRef();
        for (int originalPosition = 0; originalPosition < page.getPositionCount(); originalPosition++) {
            for (BytesRefBlock inputBlock : inputBlocks) {
                if (inputBlock.isNull(originalPosition)) {
                    result.appendNull();
                } else {
                    result.appendBytesRef(inputBlock.getBytesRef(originalPosition, spare));
                }
            }
        }

        return result.build();
    }

    private Block blockForDouble(Page page) {
        DoubleBlock.Builder result = page.getBlock(0).blockFactory().newDoubleBlockBuilder(page.getPositionCount() * sourceChannels.length);

        DoubleBlock[] inputBlocks = new DoubleBlock[sourceChannels.length];
        for (int i = 0; i < sourceChannels.length; i++) {
            inputBlocks[i] = page.getBlock(sourceChannels[i]);
        }
        for (int originalPosition = 0; originalPosition < page.getPositionCount(); originalPosition++) {
            for (DoubleBlock inputBlock : inputBlocks) {
                if (inputBlock.isNull(originalPosition)) {
                    result.appendNull();
                } else {
                    result.appendDouble(inputBlock.getDouble(originalPosition));
                }
            }
        }

        return result.build();
    }

    private Block blockForFloat(Page page) {
        return blockForDouble(page);
    }

    private Block blockForLong(Page page) {
        LongBlock.Builder result = page.getBlock(0).blockFactory().newLongBlockBuilder(page.getPositionCount() * sourceChannels.length);

        LongBlock[] inputBlocks = new LongBlock[sourceChannels.length];
        for (int i = 0; i < sourceChannels.length; i++) {
            inputBlocks[i] = page.getBlock(sourceChannels[i]);
        }
        for (int originalPosition = 0; originalPosition < page.getPositionCount(); originalPosition++) {
            for (LongBlock inputBlock : inputBlocks) {
                if (inputBlock.isNull(originalPosition)) {
                    result.appendNull();
                } else {
                    result.appendLong(inputBlock.getLong(originalPosition));
                }
            }
        }

        return result.build();
    }

    private Block blockForInt(Page page) {
        IntBlock.Builder result = page.getBlock(0).blockFactory().newIntBlockBuilder(page.getPositionCount() * sourceChannels.length);

        IntBlock[] inputBlocks = new IntBlock[sourceChannels.length];
        for (int i = 0; i < sourceChannels.length; i++) {
            inputBlocks[i] = page.getBlock(sourceChannels[i]);
        }
        for (int originalPosition = 0; originalPosition < page.getPositionCount(); originalPosition++) {
            for (IntBlock inputBlock : inputBlocks) {
                if (inputBlock.isNull(originalPosition)) {
                    result.appendNull();
                } else {
                    result.appendInt(inputBlock.getInt(originalPosition));
                }
            }
        }

        return result.build();
    }

    private Block blockForBoolean(Page page) {
        BooleanBlock.Builder result = page.getBlock(0)
            .blockFactory()
            .newBooleanBlockBuilder(page.getPositionCount() * sourceChannels.length);

        BooleanBlock[] inputBlocks = new BooleanBlock[sourceChannels.length];
        for (int i = 0; i < sourceChannels.length; i++) {
            inputBlocks[i] = page.getBlock(sourceChannels[i]);
        }
        for (int originalPosition = 0; originalPosition < page.getPositionCount(); originalPosition++) {
            for (BooleanBlock inputBlock : inputBlocks) {
                if (inputBlock.isNull(originalPosition)) {
                    result.appendNull();
                } else {
                    result.appendBoolean(inputBlock.getBoolean(originalPosition));
                }
            }
        }

        return result.build();
    }

    private boolean contains(int[] sourceChannels, int channel) {
        for (int i = 0; i < sourceChannels.length; i++) {
            if (sourceChannels[i] == channel) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "UnpivotOperator[" + "sourceChannels=" + sourceChannels + ']';
    }

}
