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
            throw new IllegalArgumentException("input must not have duplicates when [failOnDuplicate] set to [true]");
        }

        static int[] selectPositionsBefore(int[] selectedPositions, int positionCount, int untilPosition) {
            // Before the first rejection, the input page is the selection.
            if (selectedPositions.length < positionCount) {
                selectedPositions = new int[positionCount];
            }
            for (int p = 0; p < untilPosition; p++) {
                selectedPositions[p] = p;
            }
            return selectedPositions;
        }

        static Page finish(Page page) {
            return page;
        }

        static Page finish(Page page, Page output) {
            page.releaseBlocks();
            return output;
        }

        static Page finish(Page page, int[] positions, int selectedCount) {
            if (page.getPositionCount() == 0) {
                return finish(page, null);
            }
            // No buffer means nothing changed. Hand the input page to the caller as-is.
            if (positions == null) {
                return finish(page);
            }
            if (selectedCount == 0) {
                return finish(page, null);
            }
            // Build the replacement before releasing the input. Exceptions leave ownership with the operator.
            return finish(page, page.filter(false, positions, 0, selectedCount));
        }

        static Page finish(Page page, int position) {
            return finish(page, page.filter(false, position));
        }

        static boolean getAndSetOrdinal(BitArray seen, int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal key must be non-negative but was [" + ordinal + "]");
            }
            return seen.getAndSet(ordinal);
        }

        static long addOrdinalKey(
            BitArray pageLocalSeenOrdinals,
            BytesRefHashTable seenKeys,
            BytesRefVector dictionary,
            int ordinal,
            BytesRef scratch
        ) {
            // Ordinals are page-local. Hash each referenced dictionary entry once, then clear the bits.
            boolean alreadySeen = pageLocalSeenOrdinals.getAndSet(ordinal);
            if (alreadySeen) {
                return -1;
            }
            return seenKeys.add(dictionary.getBytesRef(ordinal, scratch));
        }
    }

    private static final class BytesRefProcessor implements Processor {
        final int keyChannel;
        final BytesRefHashTable seenKeys;
        final BytesRef scratch = new BytesRef();
        final BitArray pageLocalSeenOrdinals;
        int[] selectedPositions = new int[0];

        BytesRefProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.blockFactory());
        }

        BytesRefProcessor(int keyChannel, BlockFactory blockFactory) {
            this.keyChannel = keyChannel;
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            boolean success = false;
            try {
                pageLocalSeenOrdinals = new BitArray(1, blockFactory.bigArrays());
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
                // Constant vector: one key decides whether the page is dropped, passed through, or collapsed to one row.
                if (vector.isConstant()) {
                    int positionCount = page.getPositionCount();
                    if (positionCount == 0) {
                        return Processor.finish(page, null);
                    }
                    long groupId = seenKeys.add(vector.getBytesRef(0, scratch));
                    if (groupId < 0) {
                        return Processor.finish(page, null);
                    }
                    if (positionCount == 1) {
                        // no duplicates
                        return Processor.finish(page);
                    }

                    return Processor.finish(page, 0);
                }

                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                if (ordinals == null) {
                    // Plain vector path: each position contributes exactly one key.
                    return processVector(page, vector);
                }

                // Ordinal vector path: page-local ordinals avoid repeated dictionary hashing.
                return processOrdinalsVector(page, ordinals);
            }

            OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
            // Dedup keys a position by its first value; multivalued ordinal blocks must take the generic path.
            if (ordinals != null && ordinals.mayHaveMultivaluedFields() == false) {
                // Ordinal block path: single-valued ordinals with null checks and page-local dictionary deduplication.
                return processOrdinalsBlock(page, ordinals);
            }
            // Generic block path: handles nulls and multivalued positions using each position's first value.
            return processBlock(page, keyBlock);
        }

        private Page processVector(Page page, BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                long groupId = seenKeys.add(vector.getBytesRef(p, scratch));
                if (groupId >= 0) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            return Processor.finish(page, positions, selectedCount);
        }

        private Page processBlock(Page page, BytesRefBlock block) {
            int positionCount = block.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;

            for (int p = 0; p < positionCount; p++) {
                boolean keep = false;
                if (block.isNull(p) == false) {
                    int valueIndex = block.getFirstValueIndex(p);
                    long groupId = seenKeys.add(block.getBytesRef(valueIndex, scratch));
                    keep = groupId >= 0;
                }
                if (keep) {
                    if (positions != null) {
                        positions[selectedCount++] = p;
                    }
                } else if (positions == null) {
                    selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                    positions = selectedPositions;
                    selectedCount = p;
                }
            }
            return Processor.finish(page, positions, selectedCount);
        }

        private Page processOrdinalsVector(Page page, OrdinalBytesRefVector ordinals) {
            IntVector ordinalVector = ordinals.getOrdinalsVector();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            int positionCount = ordinalVector.getPositionCount();
            int[] positions = null;
            int selectedCount = 0;
            try {
                for (int p = 0; p < positionCount; p++) {
                    int ord = ordinalVector.getInt(p);
                    long groupId = Processor.addOrdinalKey(pageLocalSeenOrdinals, seenKeys, dictionary, ord, scratch);
                    if (groupId >= 0) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }
            } finally {
                pageLocalSeenOrdinals.fill(0, dictionary.getPositionCount(), false);
            }
            return Processor.finish(page, positions, selectedCount);
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
                        long groupId = Processor.addOrdinalKey(pageLocalSeenOrdinals, seenKeys, dictionary, ord, scratch);
                        keep = groupId >= 0;
                    }
                    if (keep) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }
            } finally {
                pageLocalSeenOrdinals.fill(0, dictionarySize, false);
            }
            return Processor.finish(page, positions, selectedCount);
        }

        @Override
        public String toString() {
            return "BytesRefProcessor[seenKeys=" + seenKeys.size() + "]";
        }

        @Override
        public void close() {
            Releasables.close(seenKeys, pageLocalSeenOrdinals);
        }
    }

    private static final class BytesRefFailOnDuplicateProcessor implements Processor {
        private final int keyChannel;
        private final BytesRefHashTable seenKeys;
        private final BytesRef scratch = new BytesRef();
        private final BitArray pageLocalSeenOrdinals;

        BytesRefFailOnDuplicateProcessor(int keyChannel, DriverContext driverContext) {
            this(keyChannel, driverContext.blockFactory());
        }

        BytesRefFailOnDuplicateProcessor(int keyChannel, BlockFactory blockFactory) {
            this.keyChannel = keyChannel;
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            boolean success = false;
            try {
                pageLocalSeenOrdinals = new BitArray(1, blockFactory.bigArrays());
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
                // Constant vector: any page with more than one position is a duplicate.
                if (vector.isConstant()) {
                    int positionCount = vector.getPositionCount();
                    if (positionCount != 0) {
                        long groupId = seenKeys.add(vector.getBytesRef(0, scratch));
                        if (groupId < 0 || positionCount > 1) {
                            Processor.throwOnDuplicate();
                        }
                    }
                    return Processor.finish(page);
                }
                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                if (ordinals == null) {
                    // Plain vector path: every position contributes exactly one key.
                    guardVector(vector);
                } else {
                    // Ordinal vector path: check page-local ordinal repeats before hashing dictionary values.
                    guardOrdinalsVector(ordinals);
                }
            } else {
                OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
                // Keep these block loops separate to avoid callback or branch overhead in the innermost hot path.
                if (ordinals == null) {
                    // Generic block path: every non-null value in every position must be globally unique.
                    int positionCount = keyBlock.getPositionCount();
                    // Duplicate values inside one multivalued position are still duplicates.
                    for (int p = 0; p < positionCount; p++) {
                        int first = keyBlock.getFirstValueIndex(p);
                        int end = first + keyBlock.getValueCount(p);
                        for (int valueIndex = first; valueIndex < end; valueIndex++) {
                            long groupId = seenKeys.add(keyBlock.getBytesRef(valueIndex, scratch));
                            if (groupId < 0) {
                                Processor.throwOnDuplicate();
                            }
                        }
                    }
                } else {
                    // Ordinal block path: every ordinal value must be unique after dictionary lookup.
                    IntBlock ordinalBlock = ordinals.getOrdinalsBlock();
                    BytesRefVector dictionary = ordinals.getDictionaryVector();
                    int dictionarySize = dictionary.getPositionCount();
                    try {
                        int positionCount = ordinalBlock.getPositionCount();
                        for (int p = 0; p < positionCount; p++) {
                            int first = ordinalBlock.getFirstValueIndex(p);
                            int end = first + ordinalBlock.getValueCount(p);
                            for (int valueIndex = first; valueIndex < end; valueIndex++) {
                                long groupId = Processor.addOrdinalKey(
                                    pageLocalSeenOrdinals,
                                    seenKeys,
                                    dictionary,
                                    ordinalBlock.getInt(valueIndex),
                                    scratch
                                );
                                if (groupId < 0) {
                                    Processor.throwOnDuplicate();
                                }
                            }
                        }
                    } finally {
                        pageLocalSeenOrdinals.fill(0, dictionarySize, false);
                    }
                }
            }
            return Processor.finish(page);
        }

        private void guardVector(BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                long groupId = seenKeys.add(vector.getBytesRef(p, scratch));
                if (groupId < 0) {
                    Processor.throwOnDuplicate();
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
                    long groupId = Processor.addOrdinalKey(pageLocalSeenOrdinals, seenKeys, dictionary, ordinalVector.getInt(p), scratch);
                    if (groupId < 0) {
                        Processor.throwOnDuplicate();
                    }
                }
            } finally {
                pageLocalSeenOrdinals.fill(0, dictionarySize, false);
            }
        }

        @Override
        public void close() {
            Releasables.close(seenKeys, pageLocalSeenOrdinals);
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
            if (vector == null) {
                // Block path: handles nulls and multivalued positions using each position's first ordinal.
                int positionCount = ordinalBlock.getPositionCount();
                int[] positions = null;
                int selectedCount = 0;

                for (int p = 0; p < positionCount; p++) {
                    boolean keep = false;
                    if (ordinalBlock.isNull(p) == false) {
                        boolean alreadySeen = Processor.getAndSetOrdinal(seen, ordinalBlock.getInt(ordinalBlock.getFirstValueIndex(p)));
                        keep = alreadySeen == false;
                    }
                    if (keep) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }
                return Processor.finish(page, positions, selectedCount);
            } else if (vector.isConstant()) {
                // Constant vector: one ordinal decides whether the page is dropped, passed through, or collapsed to one row.
                int positionCount = page.getPositionCount();
                if (positionCount == 0) {
                    return Processor.finish(page, null);
                }
                boolean alreadySeen = Processor.getAndSetOrdinal(seen, vector.getInt(0));
                if (alreadySeen) {
                    return Processor.finish(page, null);
                }

                if (positionCount == 1) {
                    return Processor.finish(page);
                }

                return Processor.finish(page, 0);
            } else {
                // Plain vector path: every position contributes exactly one ordinal.
                int positionCount = vector.getPositionCount();
                int[] positions = null;
                int selectedCount = 0;

                for (int p = 0; p < positionCount; p++) {
                    boolean alreadySeen = Processor.getAndSetOrdinal(seen, vector.getInt(p));
                    if (alreadySeen == false) {
                        if (positions != null) {
                            positions[selectedCount++] = p;
                        }
                    } else if (positions == null) {
                        selectedPositions = Processor.selectPositionsBefore(selectedPositions, positionCount, p);
                        positions = selectedPositions;
                        selectedCount = p;
                    }
                }

                return Processor.finish(page, positions, selectedCount);
            }
        }

        @Override
        public void close() {
            seen.close();
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
                // Block path: every non-null ordinal value in every position must be unique.
                int positionCount = ordinals.getPositionCount();
                for (int p = 0; p < positionCount; p++) {
                    int first = ordinals.getFirstValueIndex(p);
                    int end = first + ordinals.getValueCount(p);
                    for (int valueIndex = first; valueIndex < end; valueIndex++) {
                        boolean alreadySeen = Processor.getAndSetOrdinal(seen, ordinals.getInt(valueIndex));
                        if (alreadySeen) {
                            Processor.throwOnDuplicate();
                        }
                    }
                }
            } else if (vector.isConstant()) {
                // Constant vector: any page with more than one position is a duplicate.
                int positionCount = vector.getPositionCount();
                if (positionCount != 0) {
                    boolean alreadySeen = Processor.getAndSetOrdinal(seen, vector.getInt(0));
                    if (alreadySeen || positionCount > 1) {
                        Processor.throwOnDuplicate();
                    }
                }
            } else {
                // Plain vector path: every position contributes exactly one ordinal.
                int positionCount = vector.getPositionCount();
                for (int p = 0; p < positionCount; p++) {
                    boolean alreadySeen = Processor.getAndSetOrdinal(seen, vector.getInt(p));
                    if (alreadySeen) {
                        Processor.throwOnDuplicate();
                    }
                }
            }
            return Processor.finish(page);
        }

        @Override
        public void close() {
            seen.close();
        }
    }
}
