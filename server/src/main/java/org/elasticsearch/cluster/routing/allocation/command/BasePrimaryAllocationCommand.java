/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Abstract base class for allocating an unassigned primary shard to a node
 */
public abstract class BasePrimaryAllocationCommand extends AbstractAllocateAllocationCommand {

    private static final String ACCEPT_DATA_LOSS_FIELD = "accept_data_loss";

    protected static <T extends Builder<?>> ObjectParser<T, Void> createAllocatePrimaryParser(String command) {
        ObjectParser<T, Void> parser = AbstractAllocateAllocationCommand.createAllocateParser(command);
        parser.declareBoolean(Builder::setAcceptDataLoss, new ParseField(ACCEPT_DATA_LOSS_FIELD));
        return parser;
    }

    protected final boolean acceptDataLoss;

    protected BasePrimaryAllocationCommand(String index, int shardId, String node, boolean acceptDataLoss) {
        super(index, shardId, node);
        this.acceptDataLoss = acceptDataLoss;
    }

    /**
     * Read from a stream.
     */
    protected BasePrimaryAllocationCommand(StreamInput in) throws IOException {
        super(in);
        acceptDataLoss = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(acceptDataLoss);
    }

    /**
     * The operation only executes if the user explicitly agrees to possible data loss
     *
     * @return whether data loss is acceptable
     */
    public boolean acceptDataLoss() {
        return acceptDataLoss;
    }

    protected abstract static class Builder<T extends BasePrimaryAllocationCommand> extends AbstractAllocateAllocationCommand.Builder<T> {
        protected boolean acceptDataLoss;

        public void setAcceptDataLoss(boolean acceptDataLoss) {
            this.acceptDataLoss = acceptDataLoss;
        }
    }

    @Override
    protected void extraXContent(XContentBuilder builder) throws IOException {
        builder.field(ACCEPT_DATA_LOSS_FIELD, acceptDataLoss);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        BasePrimaryAllocationCommand other = (BasePrimaryAllocationCommand) obj;
        return acceptDataLoss == other.acceptDataLoss;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(acceptDataLoss);
    }
}
