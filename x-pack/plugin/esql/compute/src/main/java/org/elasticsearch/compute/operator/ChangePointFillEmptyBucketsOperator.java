/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Inserts zero-valued rows for missing date bucket keys before {@link ChangePointOperator}.
 */
public class ChangePointFillEmptyBucketsOperator implements Operator {

    public record Factory(
        int keyChannel,
        int valueChannel,
        int[] groupingChannels,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointFillEmptyBucketsOperator(
                driverContext,
                keyChannel,
                valueChannel,
                groupingChannels,
                dateBucketRounding,
                minDate,
                maxDate
            );
        }

        @Override
        public String describe() {
            return Strings.format(
                "ChangePointFillEmptyBucketsOperator[keyChannel=%d, valueChannel=%d, groupingChannels=%s]",
                keyChannel,
                valueChannel,
                Arrays.toString(groupingChannels)
            );
        }
    }

    private final DriverContext driverContext;
    private final int keyChannel;
    private final int valueChannel;
    private final int[] groupingChannels;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;

    private GroupKeyEncoder encoder;
    private PagedBytesBuilder currentGroupKeyStorage;
    private PagedBytesCursor currentGroupKeyCursor;
    private final Deque<Page> currentGroupPages;
    private final Deque<Page> outputPages;
    private boolean finished;

    public ChangePointFillEmptyBucketsOperator(
        DriverContext driverContext,
        int keyChannel,
        int valueChannel,
        int[] groupingChannels,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate
    ) {
        this.driverContext = driverContext;
        this.keyChannel = keyChannel;
        this.valueChannel = valueChannel;
        this.groupingChannels = groupingChannels;
        this.dateBucketRounding = dateBucketRounding;
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.currentGroupPages = new ArrayDeque<>();
        this.outputPages = new ArrayDeque<>();
    }

    @Override
    public boolean needsInput() {
        return finished == false && outputPages.isEmpty();
    }

    @Override
    public void addInput(Page page) {
        try {
            processPage(page);
        } catch (Exception e) {
            page.releaseBlocks();
            throw e;
        }
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            flushGroup();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (outputPages.isEmpty()) {
            return null;
        }
        return outputPages.removeFirst();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputPages.isEmpty() == false;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            () -> Releasables.close(currentGroupPages),
            () -> Releasables.close(outputPages),
            encoder,
            currentGroupKeyStorage
        );
    }

    @Override
    public String toString() {
        return "ChangePointFillEmptyBucketsOperator[keyChannel="
            + keyChannel
            + ", valueChannel="
            + valueChannel
            + ", groupingChannels="
            + Arrays.toString(groupingChannels)
            + "]";
    }

    private void processPage(Page page) {
        if (page.getPositionCount() == 0) {
            page.releaseBlocks();
            return;
        }
        if (groupingChannels.length > 0 && encoder == null) {
            initEncoderAndCurrentKeyStorage(page);
            storeCurrentGroupKey(encoder.encode(page, 0));
        }

        if (groupingChannels.length == 0) {
            currentGroupPages.add(page);
            return;
        }

        int positionCount = page.getPositionCount();
        int scanStart = 0;
        for (int i = 0; i < positionCount; i++) {
            PagedBytesCursor key = encoder.encode(page, i);
            if (key.equals(currentGroupKeyCursor)) {
                continue;
            }
            if (i > scanStart) {
                currentGroupPages.add(page.slice(scanStart, i));
            }
            flushGroup();
            scanStart = i;
            storeCurrentGroupKey(key);
        }

        if (scanStart == 0) {
            currentGroupPages.add(page);
        } else {
            currentGroupPages.add(page.slice(scanStart, positionCount));
            page.releaseBlocks();
        }
    }

    private void initEncoderAndCurrentKeyStorage(Page page) {
        List<ElementType> elementTypes = new ArrayList<>(page.getBlockCount());
        for (int i = 0; i < page.getBlockCount(); i++) {
            elementTypes.add(page.getBlock(i).elementType());
        }
        BlockFactory blockFactory = driverContext.blockFactory();
        PagedBytesBuilder encoderRow = new PagedBytesBuilder(
            blockFactory.bigArrays().recycler(),
            blockFactory.breaker(),
            "change-point-fill-group-key-encoder",
            64
        );
        encoder = new GroupKeyEncoder(groupingChannels, elementTypes, encoderRow);
        currentGroupKeyStorage = new PagedBytesBuilder(
            blockFactory.bigArrays().recycler(),
            blockFactory.breaker(),
            "change-point-fill-current-group-key",
            64
        );
        currentGroupKeyCursor = new PagedBytesCursor();
    }

    private void storeCurrentGroupKey(PagedBytesCursor freshKey) {
        currentGroupKeyStorage.clear();
        currentGroupKeyStorage.append(freshKey);
        currentGroupKeyStorage.view(currentGroupKeyCursor);
    }

    private void flushGroup() {
        if (currentGroupPages.isEmpty()) {
            return;
        }

        List<Row> rows = new ArrayList<>();
        List<Page> bufferedPages = new ArrayList<>();
        for (Page page : currentGroupPages) {
            bufferedPages.add(page);
            int positionCount = page.getPositionCount();
            for (int i = 0; i < positionCount; i++) {
                rows.add(new Row(((LongBlock) page.getBlock(keyChannel)).getLong(i), page, i));
            }
        }
        currentGroupPages.clear();

        TreeSet<Long> existingInRange = new TreeSet<>();
        Map<Long, Row> rowsByKey = new HashMap<>();
        for (Row row : rows) {
            if (row.key >= minDate && row.key < maxDate) {
                existingInRange.add(row.key);
                rowsByKey.putIfAbsent(row.key, row);
            }
        }

        List<Long> filledKeys = ChangePointBucketFillUtils.computeFilledKeys(existingInRange, dateBucketRounding, minDate, maxDate);
        if (filledKeys.size() == existingInRange.size()) {
            for (Page page : bufferedPages) {
                outputPages.add(page);
            }
            return;
        }

        Row template = rows.getFirst();
        List<Row> filledRows = new ArrayList<>(filledKeys.size());
        for (long key : filledKeys) {
            Row existing = rowsByKey.get(key);
            filledRows.add(existing != null ? existing : syntheticRow(template, key));
        }
        for (Row row : rows) {
            if (row.key < minDate || row.key >= maxDate) {
                filledRows.add(row);
            }
        }
        filledRows.sort(Comparator.comparingLong(Row::key));

        outputPages.add(buildPage(filledRows, template));
        for (Page page : bufferedPages) {
            page.releaseBlocks();
        }
    }

    private Row syntheticRow(Row template, long key) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int blockCount = template.page.getBlockCount();
        Block[] blocks = new Block[blockCount];
        boolean success = false;
        try {
            for (int channel = 0; channel < blockCount; channel++) {
                Block source = template.page.getBlock(channel);
                if (channel == keyChannel) {
                    try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
                        builder.appendLong(key);
                        blocks[channel] = builder.build();
                    }
                } else if (channel == valueChannel) {
                    blocks[channel] = zeroValueBlock(source.elementType(), blockFactory);
                } else if (isGroupingChannel(channel)) {
                    blocks[channel] = copySinglePosition(source, template.position, blockFactory);
                } else {
                    blocks[channel] = blockFactory.newConstantNullBlock(1);
                }
            }
            Page page = new Page(blocks);
            success = true;
            return new Row(key, page, 0);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private boolean isGroupingChannel(int channel) {
        for (int groupingChannel : groupingChannels) {
            if (groupingChannel == channel) {
                return true;
            }
        }
        return false;
    }

    private Block copySinglePosition(Block source, int position, BlockFactory blockFactory) {
        return switch (source.elementType()) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
                    builder.appendLong(((LongBlock) source).getLong(position));
                    yield builder.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(1)) {
                    builder.appendDouble(((DoubleBlock) source).getDouble(position));
                    yield builder.build();
                }
            }
            case BYTES_REF -> {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
                    if (source.isNull(position)) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(((BytesRefBlock) source).getBytesRef(position, new BytesRef()));
                    }
                    yield builder.build();
                }
            }
            case BOOLEAN -> {
                try (var builder = blockFactory.newBooleanBlockBuilder(1)) {
                    builder.appendBoolean(((org.elasticsearch.compute.data.BooleanBlock) source).getBoolean(position));
                    yield builder.build();
                }
            }
            case INT -> {
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
                    builder.appendInt(((IntBlock) source).getInt(position));
                    yield builder.build();
                }
            }
            default -> blockFactory.newConstantNullBlock(1);
        };
    }

    private Block zeroValueBlock(ElementType elementType, BlockFactory blockFactory) {
        return switch (elementType) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
                    builder.appendLong(0L);
                    yield builder.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(1)) {
                    builder.appendDouble(0d);
                    yield builder.build();
                }
            }
            case INT -> {
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
                    builder.appendInt(0);
                    yield builder.build();
                }
            }
            case FLOAT -> {
                try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(1)) {
                    builder.appendFloat(0f);
                    yield builder.build();
                }
            }
            default -> blockFactory.newConstantNullBlock(1);
        };
    }

    private Page buildPage(List<Row> rows, Row typeTemplate) {
        int blockCount = rows.getFirst().page.getBlockCount();
        BlockFactory blockFactory = driverContext.blockFactory();
        Block[] blocks = new Block[blockCount];
        boolean success = false;
        try {
            for (int channel = 0; channel < blockCount; channel++) {
                blocks[channel] = mergeChannel(rows, channel, blockFactory, typeTemplate.page.getBlock(channel).elementType());
            }
            Page page = new Page(blocks);
            success = true;
            for (Row row : rows) {
                if (row.page.getPositionCount() == 1) {
                    row.page.releaseBlocks();
                }
            }
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private Block mergeChannel(List<Row> rows, int channel, BlockFactory blockFactory, ElementType elementType) {
        return switch (elementType) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows.size())) {
                    for (Row row : rows) {
                        Block block = row.page.getBlock(channel);
                        if (block.isNull(row.position)) {
                            builder.appendNull();
                        } else {
                            builder.appendLong(((LongBlock) block).getLong(row.position));
                        }
                    }
                    yield builder.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rows.size())) {
                    for (Row row : rows) {
                        Block block = row.page.getBlock(channel);
                        if (block.isNull(row.position)) {
                            builder.appendNull();
                        } else {
                            builder.appendDouble(((DoubleBlock) block).getDouble(row.position));
                        }
                    }
                    yield builder.build();
                }
            }
            case BYTES_REF -> {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rows.size())) {
                    for (Row row : rows) {
                        Block block = row.page.getBlock(channel);
                        if (block.isNull(row.position)) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef(((BytesRefBlock) block).getBytesRef(row.position, new BytesRef()));
                        }
                    }
                    yield builder.build();
                }
            }
            case BOOLEAN -> {
                try (var builder = blockFactory.newBooleanBlockBuilder(rows.size())) {
                    for (Row row : rows) {
                        Block block = row.page.getBlock(channel);
                        if (block.isNull(row.position)) {
                            builder.appendNull();
                        } else {
                            builder.appendBoolean(((org.elasticsearch.compute.data.BooleanBlock) block).getBoolean(row.position));
                        }
                    }
                    yield builder.build();
                }
            }
            case INT -> {
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(rows.size())) {
                    for (Row row : rows) {
                        Block block = row.page.getBlock(channel);
                        if (block.isNull(row.position)) {
                            builder.appendNull();
                        } else {
                            builder.appendInt(((IntBlock) block).getInt(row.position));
                        }
                    }
                    yield builder.build();
                }
            }
            default -> {
                for (Row row : rows) {
                    Object value = BlockUtils.toJavaObject(row.page.getBlock(channel), row.position);
                    if (value != null) {
                        throw new IllegalArgumentException(
                            "Unsupported element type [" + elementType + "] in ChangePointFillEmptyBucketsOperator"
                        );
                    }
                }
                yield blockFactory.newConstantNullBlock(rows.size());
            }
        };
    }

    private record Row(long key, Page page, int position) {}
}
