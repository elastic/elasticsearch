/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

/**
 * A {@link LocalSupplier} that contains already filled {@link Block}s.
 */
public class ImmediateLocalSupplier implements LocalSupplier {

    private static final TransportVersion ESQL_PLAN_WITH_NO_COLUMNS = TransportVersion.fromName("esql_plan_with_no_columns");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LocalSupplier.class,
        "ImmediateSupplier",
        ImmediateLocalSupplier::new
    );

    protected Page page;

    ImmediateLocalSupplier(Page page) {
        this.page = page;
    }

    ImmediateLocalSupplier(StreamInput in) throws IOException {
        this(
            in.getTransportVersion().supports(ESQL_PLAN_WITH_NO_COLUMNS)
                ? new Page(in.readInt(), ((PlanStreamInput) in).readCachedBlockArray())
                : legacyPage((PlanStreamInput) in)
        );
    }

    private static Page legacyPage(PlanStreamInput in) throws IOException {
        Block[] blocks = in.readCachedBlockArray();
        if (blocks.length == 0) {
            // the page can't determine the position count from an empty array
            return new Page(0, blocks);
        }
        return new Page(blocks);
    }

    @Override
    public Page get() {
        return page;
    }

    @Override
    public String toString() {
        return page.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_PLAN_WITH_NO_COLUMNS)) {
            out.writeInt(page.getPositionCount());
        }

        Block[] blocks = new Block[page.getBlockCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
        }
        out.writeArray((o, v) -> ((PlanStreamOutput) o).writeCachedBlock(v), blocks);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ImmediateLocalSupplier other = (ImmediateLocalSupplier) obj;
        return page.equals(other.page);
    }

    @Override
    public int hashCode() {
        return page.hashCode();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
