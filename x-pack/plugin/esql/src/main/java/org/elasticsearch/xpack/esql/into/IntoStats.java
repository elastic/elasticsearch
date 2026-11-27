/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * The one-row result of an {@code INTO} command: how many rows were sent, how many the destination created, how many it
 * overwrote ({@code collisions: replace}), how many it left out because their {@code _id} already existed ({@code collisions: keep}),
 * and how many it refused under {@code keep_going}, with the reasons for a sample of those refusals, at most
 * {@link #MAX_FAILURE_DETAILS}.
 * The counts are exact. The details are a bounded sample, not the first refusals of the query: each writer keeps its first
 * reasons and a merge keeps the first ones of the stats it is given first, so a writer that reports later can lose all of its
 * details, even if its rows were refused before those of the writer that reported first.
 */
public record IntoStats(long total, long created, long updated, long versionConflicts, long failures, List<String> failureDetails) {
    // TODO: keep the most frequent distinct reasons, with their counts, instead of the first ones. Identical reasons then stop using up
    // the slots, and the merge no longer depends on which writer reports first.
    public static final int MAX_FAILURE_DETAILS = 20;

    public static final IntoStats EMPTY = builder().build();

    public IntoStats {
        failureDetails = List.copyOf(
            failureDetails.size() > MAX_FAILURE_DETAILS ? failureDetails.subList(0, MAX_FAILURE_DETAILS) : failureDetails
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<Attribute> createAttributes(Source source) {
        return List.of(
            new ReferenceAttribute(source, null, "total", DataType.LONG),
            new ReferenceAttribute(source, null, "created", DataType.LONG),
            new ReferenceAttribute(source, null, "updated", DataType.LONG),
            new ReferenceAttribute(source, null, "version_conflicts", DataType.LONG),
            new ReferenceAttribute(source, null, "failures", DataType.LONG),
            new ReferenceAttribute(source, null, "failure_details", DataType.KEYWORD)
        );
    }

    /** Reads the stats at {@code position} of a page laid out as {@link #createAttributes}. */
    static IntoStats fromPage(Page page, int position) {
        return builder().total(longAt(page.getBlock(0), position))
            .created(longAt(page.getBlock(1), position))
            .updated(longAt(page.getBlock(2), position))
            .versionConflicts(longAt(page.getBlock(3), position))
            .failures(longAt(page.getBlock(4), position))
            .failureDetails(stringsAt(page.getBlock(5), position))
            .build();
    }

    private static long longAt(LongBlock block, int position) {
        return block.getLong(block.getFirstValueIndex(position));
    }

    private static List<String> stringsAt(BytesRefBlock block, int position) {
        if (block.isNull(position)) {
            return List.of();
        }
        int first = block.getFirstValueIndex(position);
        int count = block.getValueCount(position);
        List<String> values = new ArrayList<>(count);
        BytesRef scratch = new BytesRef();
        for (int i = first; i < first + count; i++) {
            values.add(block.getBytesRef(i, scratch).utf8ToString());
        }
        return values;
    }

    /** Adds up the counters and keeps the first {@link #MAX_FAILURE_DETAILS} details of the two, this one's first. */
    IntoStats merge(IntoStats other) {
        return builder().total(total + other.total)
            .created(created + other.created)
            .updated(updated + other.updated)
            .versionConflicts(versionConflicts + other.versionConflicts)
            .failures(failures + other.failures)
            .failureDetails(CollectionUtils.concatLists(failureDetails, other.failureDetails))
            .build();
    }

    Page toPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[6];
        boolean success = false;
        try {
            blocks[0] = blockFactory.newConstantLongBlockWith(total, 1);
            blocks[1] = blockFactory.newConstantLongBlockWith(created, 1);
            blocks[2] = blockFactory.newConstantLongBlockWith(updated, 1);
            blocks[3] = blockFactory.newConstantLongBlockWith(versionConflicts, 1);
            blocks[4] = blockFactory.newConstantLongBlockWith(failures, 1);
            blocks[5] = failureDetailsBlock(blockFactory);
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private Block failureDetailsBlock(BlockFactory blockFactory) {
        if (failureDetails.isEmpty()) {
            return blockFactory.newConstantNullBlock(1);
        }
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(failureDetails.size())) {
            builder.beginPositionEntry();
            for (String detail : failureDetails) {
                builder.appendBytesRef(new BytesRef(detail));
            }
            builder.endPositionEntry();
            return builder.build();
        }
    }

    /** Every counter starts at zero and the details start empty, so a caller names only what is not. */
    public static final class Builder {
        private long total;
        private long created;
        private long updated;
        private long versionConflicts;
        private long failures;
        private List<String> failureDetails = List.of();

        private Builder() {}

        public Builder total(long total) {
            this.total = total;
            return this;
        }

        public Builder created(long created) {
            this.created = created;
            return this;
        }

        public Builder updated(long updated) {
            this.updated = updated;
            return this;
        }

        public Builder versionConflicts(long versionConflicts) {
            this.versionConflicts = versionConflicts;
            return this;
        }

        public Builder failures(long failures) {
            this.failures = failures;
            return this;
        }

        public Builder failureDetails(List<String> failureDetails) {
            this.failureDetails = failureDetails;
            return this;
        }

        public IntoStats build() {
            return new IntoStats(total, created, updated, versionConflicts, failures, failureDetails);
        }
    }
}
