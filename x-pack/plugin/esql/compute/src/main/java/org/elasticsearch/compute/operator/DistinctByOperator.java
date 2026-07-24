/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Operator that tracks the distinct values of a single key column across pages.
 * It either filters repeated keys or acts as a pass-through uniqueness guard.
 * Null key positions are never treated as duplicates.
 */
public final class DistinctByOperator extends AbstractPageMappingOperator {

    /** Builds a {@link DistinctByOperator} for a {@code BYTES_REF} key. */
    public record BytesRefKeyFactory(int keyChannel, boolean failOnDuplicate) implements OperatorFactory {

        public BytesRefKeyFactory(int keyChannel) {
            this(keyChannel, false);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new DistinctByOperator(
                keyChannel(),
                failOnDuplicate()
                    ? new BytesRefFailOnDuplicateProcessor(keyChannel(), driverContext)
                    : new BytesRefProcessor(keyChannel(), driverContext)
            );
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel="
                + keyChannel()
                + ", failOnDuplicate="
                + failOnDuplicate()
                + ", factory="
                + this.getClass().getSimpleName()
                + "]";
        }
    }

    /** Builds a {@link DistinctByOperator} for a compact, non-negative integer ordinal key. */
    public record OrdinalIntKeyFactory(int keyChannel, boolean failOnDuplicate) implements OperatorFactory {

        public OrdinalIntKeyFactory(int keyChannel) {
            this(keyChannel, false);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new DistinctByOperator(
                keyChannel(),
                failOnDuplicate()
                    ? new IntOrdinalFailOnDuplicateProcessor(keyChannel(), driverContext)
                    : new IntOrdinalProcessor(keyChannel(), driverContext)
            );
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel="
                + keyChannel()
                + ", failOnDuplicate="
                + failOnDuplicate()
                + ", factory="
                + this.getClass().getSimpleName()
                + "]";
        }
    }

    private final int keyChannel;
    private final Processor processor;

    private DistinctByOperator(int keyChannel, Processor processor) {
        this.keyChannel = keyChannel;
        this.processor = processor;
    }

    @Override
    protected Page process(Page page) {
        return processor.process(page);
    }

    @Override
    public String toString() {
        return "DistinctByOperator[keyChannel=" + keyChannel + ", processor=" + processor + "]";
    }

    @Override
    public void close() {
        Releasables.close(processor, super::close);
    }

    private sealed interface Processor extends Releasable permits BytesRefProcessor, BytesRefFailOnDuplicateProcessor, IntOrdinalProcessor,
        IntOrdinalFailOnDuplicateProcessor {
        Page process(Page page);

        static void throwOnDuplicate() {
            throw new IllegalArgumentException("input channel cannot emit duplicate keys when [failOnDuplicate] is [true]");
        }
    }

    private static final class BytesRefProcessor implements Processor {
        final int keyChannel;
        final BytesRefHashTable seenKeys;
        final BytesRef scratch = new BytesRef();
        final BitArray pageSeenOrdinals;
        int[] selectedPositions = new int[0];

        BytesRefProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.blockFactory());
        }

        BytesRefProcessor(int keyChannel, BlockFactory blockFactory) {
            this.keyChannel = keyChannel;
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            boolean success = false;
            try {
                pageSeenOrdinals = new BitArray(1, blockFactory.bigArrays());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(seenKeys);
                }
            }
        }

        @Override
        public Page process(Page page) {
            Page result = doProcess(page);
            page.releaseBlocks();
            return result;
        }

        private Page doProcess(Page page) {
            BytesRefBlock keyBlock = page.getBlock(keyChannel);
            BytesRefVector vector = keyBlock.asVector();
            if (vector != null) {
                if (vector.isConstant()) {
                    return processConstantVector(page, vector);
                }
                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                return ordinals == null ? processVector(page, vector) : processOrdinalsVector(page, ordinals);
            }

            OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
            if (ordinals != null && ordinals.mayHaveMultivaluedFields() == false) {
                return processOrdinalsBlock(page, ordinals);
            }
            return processBlock(page, keyBlock);
        }

        private Page processConstantVector(Page page, BytesRefVector vector) {
            int positionCount = page.getPositionCount();
            if (positionCount == 0) {
                return null;
            }
            if (seenKeys.add(vector.getBytesRef(0, scratch)) < 0) {
                return null;
            }
            return positionCount == 1 ? page.shallowCopy() : page.filter(false, 0);
        }

        private Page processVector(Page page, BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                if (seenKeys.add(vector.getBytesRef(p, scratch)) >= 0) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    if (selectedPositions.length < positionCount) {
                        selectedPositions = new int[positionCount];
                    }
                    for (int p1 = 0; p1 < p; p1++) {
                        selectedPositions[p1] = p1;
                    }
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        private Page processBlock(Page page, BytesRefBlock block) {
            int positionCount = block.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                boolean keep = block.isNull(p) == false && seenKeys.add(block.getBytesRef(block.getFirstValueIndex(p), scratch)) >= 0;
                if (keep) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    if (selectedPositions.length < positionCount) {
                        selectedPositions = new int[positionCount];
                    }
                    for (int p1 = 0; p1 < p; p1++) {
                        selectedPositions[p1] = p1;
                    }
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        private Page processOrdinalsVector(Page page, OrdinalBytesRefVector ordinals) {
            IntVector ordinalVector = ordinals.getOrdinalsVector();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            int positionCount = ordinalVector.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;
            int dictionarySize = dictionary.getPositionCount();
            try {
                for (int p = 0; p < positionCount; p++) {
                    int ord = ordinalVector.getInt(p);
                    boolean keep = false;
                    if (pageSeenOrdinals.getAndSet(ord) == false) {
                        keep = seenKeys.add(dictionary.getBytesRef(ord, scratch)) >= 0;
                    }
                    if (keep) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        if (selectedPositions.length < positionCount) {
                            selectedPositions = new int[positionCount];
                        }
                        for (int p1 = 0; p1 < p; p1++) {
                            selectedPositions[p1] = p1;
                        }
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }
            } finally {
                clearPageSeenOrdinals(dictionarySize);
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        private Page processOrdinalsBlock(Page page, OrdinalBytesRefBlock ordinals) {
            IntBlock ordinalBlock = ordinals.getOrdinalsBlock();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            int positionCount = ordinalBlock.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;
            int dictionarySize = dictionary.getPositionCount();
            try {
                for (int p = 0; p < positionCount; p++) {
                    boolean keep = false;
                    if (ordinalBlock.isNull(p) == false) {
                        int ord = ordinalBlock.getInt(ordinalBlock.getFirstValueIndex(p));
                        if (pageSeenOrdinals.getAndSet(ord) == false) {
                            keep = seenKeys.add(dictionary.getBytesRef(ord, scratch)) >= 0;
                        }
                    }
                    if (keep) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        if (selectedPositions.length < positionCount) {
                            selectedPositions = new int[positionCount];
                        }
                        for (int p1 = 0; p1 < p; p1++) {
                            selectedPositions[p1] = p1;
                        }
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }
            } finally {
                clearPageSeenOrdinals(dictionarySize);
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        @Override
        public String toString() {
            return "BytesRefProcessor[seenKeys=" + seenKeys.size() + "]";
        }

        @Override
        public void close() {
            Releasables.close(seenKeys, pageSeenOrdinals);
        }

        private void clearPageSeenOrdinals(int dictionarySize) {
            pageSeenOrdinals.fill(0, dictionarySize, false);
        }
    }

    private static final class BytesRefFailOnDuplicateProcessor implements Processor {
        private final int keyChannel;
        private final BytesRefHashTable seenKeys;
        private final BytesRef scratch = new BytesRef();
        private final BitArray pageSeenOrdinals;

        BytesRefFailOnDuplicateProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.blockFactory());
        }

        BytesRefFailOnDuplicateProcessor(int keyChannel, BlockFactory blockFactory) {
            this.keyChannel = keyChannel;
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            boolean success = false;
            try {
                pageSeenOrdinals = new BitArray(1, blockFactory.bigArrays());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(seenKeys);
                }
            }
        }

        @Override
        public Page process(Page page) {
            BytesRefBlock keyBlock = page.getBlock(keyChannel);
            BytesRefVector vector = keyBlock.asVector();
            if (vector != null) {
                if (vector.isConstant()) {
                    guardConstantVector(vector);
                    return page;
                }
                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                if (ordinals == null) {
                    guardVector(vector);
                } else {
                    guardOrdinalsVector(ordinals);
                }
            } else {
                OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
                if (ordinals == null) {
                    guardBlock(keyBlock);
                } else {
                    guardOrdinalsBlock(ordinals);
                }
            }
            return page;
        }

        private void guardConstantVector(BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            if (positionCount == 0) {
                return;
            }
            if (seenKeys.add(vector.getBytesRef(0, scratch)) < 0 || positionCount > 1) {
                Processor.throwOnDuplicate();
            }
        }

        private void guardVector(BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                if (seenKeys.add(vector.getBytesRef(p, scratch)) < 0) {
                    Processor.throwOnDuplicate();
                }
            }
        }

        private void guardBlock(BytesRefBlock block) {
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                int first = block.getFirstValueIndex(p);
                int end = first + block.getValueCount(p);
                for (int valueIndex = first; valueIndex < end; valueIndex++) {
                    if (seenKeys.add(block.getBytesRef(valueIndex, scratch)) < 0) {
                        Processor.throwOnDuplicate();
                    }
                }
            }
        }

        private void guardOrdinalsVector(OrdinalBytesRefVector ordinals) {
            IntVector ordinalVector = ordinals.getOrdinalsVector();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            int dictionarySize = dictionary.getPositionCount();
            try {
                int positionCount = ordinalVector.getPositionCount();
                for (int p = 0; p < positionCount; p++) {
                    guardOrdinal(dictionary, ordinalVector.getInt(p));
                }
            } finally {
                clearPageSeenOrdinals(dictionarySize);
            }
        }

        private void guardOrdinalsBlock(OrdinalBytesRefBlock ordinals) {
            IntBlock ordinalBlock = ordinals.getOrdinalsBlock();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            int dictionarySize = dictionary.getPositionCount();
            try {
                int positionCount = ordinalBlock.getPositionCount();
                for (int p = 0; p < positionCount; p++) {
                    int first = ordinalBlock.getFirstValueIndex(p);
                    int end = first + ordinalBlock.getValueCount(p);
                    for (int valueIndex = first; valueIndex < end; valueIndex++) {
                        guardOrdinal(dictionary, ordinalBlock.getInt(valueIndex));
                    }
                }
            } finally {
                clearPageSeenOrdinals(dictionarySize);
            }
        }

        private void guardOrdinal(BytesRefVector dictionary, int ord) {
            if (pageSeenOrdinals.getAndSet(ord)) {
                Processor.throwOnDuplicate();
            }
            if (seenKeys.add(dictionary.getBytesRef(ord, scratch)) < 0) {
                Processor.throwOnDuplicate();
            }
        }

        @Override
        public void close() {
            Releasables.close(seenKeys, pageSeenOrdinals);
        }

        private void clearPageSeenOrdinals(int dictionarySize) {
            pageSeenOrdinals.fill(0, dictionarySize, false);
        }
    }

    private static final class IntOrdinalProcessor implements Processor {
        private final int keyChannel;
        private final BitArray seen;
        private int[] selectedPositions = new int[0];

        IntOrdinalProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.bigArrays());
        }

        IntOrdinalProcessor(int keyChannel, BigArrays bigArrays) {
            this.keyChannel = keyChannel;
            this.seen = new BitArray(1, bigArrays);
        }

        @Override
        public Page process(Page page) {
            IntBlock ordinalBlock = page.getBlock(keyChannel);
            IntVector vector = ordinalBlock.asVector();
            Page result = vector == null ? processBlock(page, ordinalBlock) : processVector(page, vector);
            page.releaseBlocks();
            return result;
        }

        private Page processVector(Page page, IntVector vector) {
            int positionCount = vector.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                if (getAndSetOrdinal(vector.getInt(p)) == false) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    if (selectedPositions.length < positionCount) {
                        selectedPositions = new int[positionCount];
                    }
                    for (int p1 = 0; p1 < p; p1++) {
                        selectedPositions[p1] = p1;
                    }
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        private Page processBlock(Page page, IntBlock block) {
            int positionCount = block.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                boolean keep = block.isNull(p) == false && getAndSetOrdinal(block.getInt(block.getFirstValueIndex(p))) == false;
                if (keep) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    if (selectedPositions.length < positionCount) {
                        selectedPositions = new int[positionCount];
                    }
                    for (int p1 = 0; p1 < p; p1++) {
                        selectedPositions[p1] = p1;
                    }
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            if (page.getPositionCount() == 0) {
                return null;
            }
            if (positions == null) {
                return page.shallowCopy();
            }
            if (selectedCount == 0) {
                return null;
            }
            return page.filter(false, positions, 0, selectedCount);
        }

        @Override
        public void close() {
            seen.close();
        }

        private boolean getAndSetOrdinal(int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal key must be non-negative but was [" + ordinal + "]");
            }
            return seen.getAndSet(ordinal);
        }
    }

    private record IntOrdinalFailOnDuplicateProcessor(int keyChannel, BitArray seen) implements Processor {
        IntOrdinalFailOnDuplicateProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.bigArrays());
        }

        IntOrdinalFailOnDuplicateProcessor(int keyChannel, BigArrays bigArrays) {
            this(keyChannel, new BitArray(1, bigArrays));
        }

        @Override
        public Page process(Page page) {
            IntBlock ordinals = page.getBlock(keyChannel);
            IntVector vector = ordinals.asVector();
            if (vector == null) {
                guardBlock(ordinals);
            } else {
                guardVector(vector);
            }
            return page;
        }

        private void guardVector(IntVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                if (getAndSetOrdinal(vector.getInt(p))) {
                    Processor.throwOnDuplicate();
                }
            }
        }

        private void guardBlock(IntBlock ordinals) {
            int positionCount = ordinals.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                int first = ordinals.getFirstValueIndex(p);
                int end = first + ordinals.getValueCount(p);
                for (int valueIndex = first; valueIndex < end; valueIndex++) {
                    if (getAndSetOrdinal(ordinals.getInt(valueIndex))) {
                        Processor.throwOnDuplicate();
                    }
                }
            }
        }

        @Override
        public void close() {
            seen.close();
        }

        private boolean getAndSetOrdinal(int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal key must be non-negative but was [" + ordinal + "]");
            }
            return seen.getAndSet(ordinal);
        }
    }

}
