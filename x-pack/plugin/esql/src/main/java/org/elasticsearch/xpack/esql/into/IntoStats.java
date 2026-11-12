/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * Summary counts returned as the single result row of an {@code INTO} query.
 */
public final class IntoStats {

    public static final String TOTAL = "total";
    public static final String CREATED = "created";

    public static final List<Attribute> ATTRIBUTES = List.of(
        new ReferenceAttribute(Source.EMPTY, TOTAL, DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, CREATED, DataType.LONG)
    );

    private long total;
    private long created;

    public long total() {
        return total;
    }

    public long created() {
        return created;
    }

    void countRows(long rows) {
        total += rows;
    }

    void countCreated() {
        created++;
    }

    public Page toPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[ATTRIBUTES.size()];
        boolean success = false;
        try {
            blocks[0] = blockFactory.newConstantLongBlockWith(total, 1);
            blocks[1] = blockFactory.newConstantLongBlockWith(created, 1);
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }
}
