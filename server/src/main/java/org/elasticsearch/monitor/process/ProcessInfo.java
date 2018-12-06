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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ProcessInfo implements Writeable, ToXContentFragment {

    private final long refreshInterval;
    private final long id;
    private final boolean mlockall;

    public ProcessInfo(long id, boolean mlockall, long refreshInterval) {
        this.id = id;
        this.mlockall = mlockall;
        this.refreshInterval = refreshInterval;
    }

    public ProcessInfo(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
        id = in.readLong();
        mlockall = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        out.writeLong(id);
        out.writeBoolean(mlockall);
    }

    public long refreshInterval() {
        return this.refreshInterval;
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    /**
     * The process id.
     */
    public long getId() {
        return id;
    }

    public boolean isMlockall() {
        return mlockall;
    }

    static final class Fields {
        static final String PROCESS = "process";
        static final String REFRESH_INTERVAL = "refresh_interval";
        static final String REFRESH_INTERVAL_IN_MILLIS = "refresh_interval_in_millis";
        static final String ID = "id";
        static final String MLOCKALL = "mlockall";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PROCESS);
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, new TimeValue(refreshInterval));
        builder.field(Fields.ID, id);
        builder.field(Fields.MLOCKALL, mlockall);
        builder.endObject();
        return builder;
    }
}
