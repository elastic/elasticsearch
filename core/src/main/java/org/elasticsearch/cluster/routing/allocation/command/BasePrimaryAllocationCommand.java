/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
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

    protected static <T extends Builder<?>> ObjectParser<T, ParseFieldMatcherSupplier> createAllocatePrimaryParser(String command) {
        ObjectParser<T, ParseFieldMatcherSupplier> parser = AbstractAllocateAllocationCommand.createAllocateParser(command);
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

    protected static abstract class Builder<T extends BasePrimaryAllocationCommand> extends AbstractAllocateAllocationCommand.Builder<T> {
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
