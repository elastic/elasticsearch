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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class MigrateIndexResponse extends AcknowledgedResponse implements ToXContent {
    private boolean noop; // NOCOMMIT we can do better. Probably like reindex, make the task status useful and capture that in the response.
    
    /**
     * Constructor for use with serialization.
     */
    public MigrateIndexResponse() {
    }

    public MigrateIndexResponse(boolean acknowledged, boolean noop) {
        super(acknowledged); // NOCOMMIT right now this is always true, that is silly. We shouldn't have it if it is always true.
        this.noop = noop;
    }

    public boolean isNoop() {
        return noop;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        noop = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(noop);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("acknowledged", isAcknowledged());
        builder.field("noop", isNoop());
        return builder;
    }
}
