/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * What an {@code INTO} query reports instead of the rows it wrote: the counts a reindex reports, as one row.
 * <p>
 * Mutated by {@link IntoPageWriter} under its own lock as bulk responses arrive, and read once the writer is finished.
 */
public final class IntoStats {

    /**
     * How many failure messages are kept, in the order they arrived; the count in {@link #failures()} is not capped.
     * A later version may keep the most frequent causes instead of the first ones.
     */
    public static final int MAX_FAILURE_MESSAGES = 20;

    public static final String TOTAL = "total";
    public static final String CREATED = "created";
    public static final String UPDATED = "updated";
    public static final String VERSION_CONFLICTS = "version_conflicts";
    public static final String BATCHES = "batches";
    public static final String FAILURES = "failures";
    public static final String FAILURE_MESSAGES = "failure_messages";

    /** The schema of the row an {@code INTO} query returns, in the order the columns appear. */
    public static final List<Attribute> ATTRIBUTES = List.of(
        new ReferenceAttribute(Source.EMPTY, TOTAL, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, CREATED, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, UPDATED, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, VERSION_CONFLICTS, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, BATCHES, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, FAILURES, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, FAILURE_MESSAGES, DataType.KEYWORD)
    );

    private long total;
    private long created;
    private long updated;
    private long versionConflicts;
    private long batches;
    private long failures;
    private final List<String> failureMessages = new ArrayList<>();

    /** Rows the query produced, whether or not they were written. */
    public long total() {
        return total;
    }

    /** Documents the destination reported as created. */
    public long created() {
        return created;
    }

    /** Documents the destination reported as replaced, which only happens with {@code collisions=replace}. */
    public long updated() {
        return updated;
    }

    /** Rows skipped because their document already existed, which only happens with {@code collisions=keep}. */
    public long versionConflicts() {
        return versionConflicts;
    }

    /** Bulk requests sent. */
    public long batches() {
        return batches;
    }

    /** Rows the destination refused for a reason other than a collision, skipped because of {@code keep_going}. */
    public long failures() {
        return failures;
    }

    /** The first {@link #MAX_FAILURE_MESSAGES} reasons rows were refused. */
    public List<String> failureMessages() {
        return List.copyOf(failureMessages);
    }

    void countRows(long rows) {
        total += rows;
    }

    void countCreated() {
        created++;
    }

    void countUpdated() {
        updated++;
    }

    void countVersionConflict() {
        versionConflicts++;
    }

    void countBatch() {
        batches++;
    }

    void countFailed(String message) {
        failures++;
        if (failureMessages.size() < MAX_FAILURE_MESSAGES) {
            failureMessages.add(message);
        }
    }

    /** The single row an {@code INTO} query returns, laid out as {@link #ATTRIBUTES}. */
    public Page toPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[ATTRIBUTES.size()];
        boolean success = false;
        try {
            blocks[0] = blockFactory.newConstantLongBlockWith(total, 1);
            blocks[1] = blockFactory.newConstantLongBlockWith(created, 1);
            blocks[2] = blockFactory.newConstantLongBlockWith(updated, 1);
            blocks[3] = blockFactory.newConstantLongBlockWith(versionConflicts, 1);
            blocks[4] = blockFactory.newConstantLongBlockWith(batches, 1);
            blocks[5] = blockFactory.newConstantLongBlockWith(failures, 1);
            blocks[6] = failureMessagesBlock(blockFactory);
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private Block failureMessagesBlock(BlockFactory blockFactory) {
        if (failureMessages.isEmpty()) {
            return blockFactory.newConstantNullBlock(1);
        }
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(failureMessages.size())) {
            builder.beginPositionEntry();
            for (String message : failureMessages) {
                builder.appendBytesRef(new BytesRef(message));
            }
            builder.endPositionEntry();
            return builder.build();
        }
    }

    @Override
    public String toString() {
        return "IntoStats[total="
            + total
            + ", created="
            + created
            + ", updated="
            + updated
            + ", version_conflicts="
            + versionConflicts
            + ", batches="
            + batches
            + ", failures="
            + failures
            + "]";
    }
}
