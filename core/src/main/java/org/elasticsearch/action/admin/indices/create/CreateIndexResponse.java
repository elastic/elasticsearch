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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends AcknowledgedResponse {

    private boolean shardsAcked;

    protected CreateIndexResponse() {
    }

    protected CreateIndexResponse(boolean acknowledged, boolean shardsAcked) {
        super(acknowledged);
        assert acknowledged || shardsAcked == false; // if its not acknowledged, then shards acked should be false too
        this.shardsAcked = shardsAcked;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        shardsAcked = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(shardsAcked);
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation.  If {@link #isAcknowledged()}
     * is false, then this also returns false.
     */
    public boolean isShardsAcked() {
        return shardsAcked;
    }

    public void addCustomFields(XContentBuilder builder) throws IOException {
        builder.field("shards_acknowledged", isShardsAcked());
    }
}
