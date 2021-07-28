/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ShardsAcknowledgedResponse extends AcknowledgedResponse {

    protected static final ParseField SHARDS_ACKNOWLEDGED = new ParseField("shards_acknowledged");

    protected static <T extends ShardsAcknowledgedResponse> void declareAcknowledgedAndShardsAcknowledgedFields(
            ConstructingObjectParser<T, Void> objectParser) {
        declareAcknowledgedField(objectParser);
        objectParser.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    public static final ShardsAcknowledgedResponse NOT_ACKNOWLEDGED = new ShardsAcknowledgedResponse(false, false);
    private static final ShardsAcknowledgedResponse SHARDS_NOT_ACKNOWLEDGED = new ShardsAcknowledgedResponse(true, false);
    private static final ShardsAcknowledgedResponse ACKNOWLEDGED = new ShardsAcknowledgedResponse(true, true);

    private final boolean shardsAcknowledged;

    protected ShardsAcknowledgedResponse(StreamInput in, boolean readShardsAcknowledged) throws IOException {
        super(in);
        if (readShardsAcknowledged) {
            this.shardsAcknowledged = in.readBoolean();
        } else {
            this.shardsAcknowledged = false;
        }
    }

    public static ShardsAcknowledgedResponse of(boolean acknowledged, boolean shardsAcknowledged) {
        if (acknowledged) {
            return shardsAcknowledged ? ACKNOWLEDGED : SHARDS_NOT_ACKNOWLEDGED;
        } else {
            assert shardsAcknowledged == false;
            return NOT_ACKNOWLEDGED;
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
