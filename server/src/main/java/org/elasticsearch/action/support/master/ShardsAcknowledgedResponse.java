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

package org.elasticsearch.action.support.master;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public abstract class ShardsAcknowledgedResponse extends AcknowledgedResponse {

    protected static final ParseField SHARDS_ACKNOWLEDGED = new ParseField("shards_acknowledged");

    protected static <T extends ShardsAcknowledgedResponse> void declareAcknowledgedAndShardsAcknowledgedFields(
            ConstructingObjectParser<T, Void> objectParser) {
        declareAcknowledgedField(objectParser);
        objectParser.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    private final boolean shardsAcknowledged;

    protected ShardsAcknowledgedResponse(StreamInput in, boolean readShardsAcknowledged) throws IOException {
        super(in);
        if (readShardsAcknowledged) {
            this.shardsAcknowledged = in.readBoolean();
        } else {
            this.shardsAcknowledged = false;
        }
    }

    protected ShardsAcknowledgedResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged);
        assert acknowledged || shardsAcknowledged == false; // if it's not acknowledged, then shards acked should be false too
        this.shardsAcknowledged = shardsAcknowledged;
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation. If {@link #isAcknowledged()}
     * is false, then this also returns false.
     */
    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    protected void writeShardsAcknowledged(StreamOutput out) throws IOException {
        out.writeBoolean(shardsAcknowledged);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(SHARDS_ACKNOWLEDGED.getPreferredName(), isShardsAcknowledged());
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ShardsAcknowledgedResponse that = (ShardsAcknowledgedResponse) o;
            return isShardsAcknowledged() == that.isShardsAcknowledged();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isShardsAcknowledged());
    }

}
