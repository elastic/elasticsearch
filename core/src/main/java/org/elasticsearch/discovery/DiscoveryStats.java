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

package org.elasticsearch.discovery;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.zen.publish.PendingClusterStateStats;

import java.io.IOException;

public class DiscoveryStats implements Streamable, ToXContent {

    @Nullable
    private PendingClusterStateStats queueStats;

    public DiscoveryStats(PendingClusterStateStats queueStats) {
        this.queueStats = queueStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DISCOVERY);

        if (queueStats != null ){
            queueStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            queueStats = new PendingClusterStateStats();
            queueStats.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (queueStats != null ) {
            out.writeBoolean(true);
            queueStats.writeTo(out);
        }else{
            out.writeBoolean(false);
        }
    }

    static final class Fields {
        static final String DISCOVERY = "discovery";
    }

    public PendingClusterStateStats getQueueStats() {
        return queueStats;
    }
}
