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
 * The one-row result of an {@code INTO} command: how many rows were sent, how many the destination created, and how many
 * it refused under {@code keep_going}, with the reasons for a sample of those refusals, at most {@link #MAX_FAILURE_DETAILS}.
 * The counts are exact. The details are a bounded sample, not the first refusals of the query: each writer keeps its first
 * reasons and a merge keeps the first ones of the stats it is given first, so a writer that reports later can lose all of its
 * details, even if its rows were refused before those of the writer that reported first.
 */
public record IntoStats(long total, long created, long failures, List<String> failureDetails) {
    // TODO: keep the most frequent distinct reasons, with their counts, instead of the first ones. Identical reasons then stop using up
    // the slots, and the merge no longer depends on which writer reports first.
    public static final int MAX_FAILURE_DETAILS = 20;

    public static final IntoStats EMPTY = new IntoStats(0, 0, 0, List.of());

    public IntoStats {
        failureDetails = List.copyOf(
            failureDetails.size() > MAX_FAILURE_DETAILS ? failureDetails.subList(0, MAX_FAILURE_DETAILS) : failureDetails
        );
    }

    public static List<Attribute> createAttributes(Source source) {
        return List.of(
            new ReferenceAttribute(source, null, "total", DataType.LONG),
            new ReferenceAttribute(source, null, "created", DataType.LONG),
            new ReferenceAttribute(source, null, "failures", DataType.LONG),
            new ReferenceAttribute(source, null, "failure_details", DataType.KEYWORD)
        );
    }

    /** Reads the stats at {@code position} of a page laid out as {@link #createAttributes}. */
    static IntoStats fromPage(Page page, int position) {
        return new IntoStats(
            longAt(page.getBlock(0), position),
            longAt(page.getBlock(1), position),
            longAt(page.getBlock(2), position),
            stringsAt(page.getBlock(3), position)
        );
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
        List<String> details = CollectionUtils.concatLists(failureDetails, other.failureDetails);
        return new IntoStats(total + other.total, created + other.created, failures + other.failures, details);
    }

    Page toPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[4];
        boolean success = false;
        try {
            blocks[0] = blockFactory.newConstantLongBlockWith(total, 1);
            blocks[1] = blockFactory.newConstantLongBlockWith(created, 1);
            blocks[2] = blockFactory.newConstantLongBlockWith(failures, 1);
            blocks[3] = failureDetailsBlock(blockFactory);
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
}
