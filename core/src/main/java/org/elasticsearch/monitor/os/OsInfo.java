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

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class OsInfo implements Streamable, ToXContent {

    long refreshInterval;

    int availableProcessors;

    int allocatedProcessors;

    String name = null;
    String arch = null;
    String version = null;

    OsInfo() {
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    public int getAvailableProcessors() {
        return this.availableProcessors;
    }

    public int getAllocatedProcessors() {
        return this.allocatedProcessors;
    }

    public String getName() {
        return name;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }

    static final class Fields {
        static final String OS = "os";
        static final String NAME = "name";
        static final String ARCH = "arch";
        static final String VERSION = "version";
        static final String REFRESH_INTERVAL = "refresh_interval";
        static final String REFRESH_INTERVAL_IN_MILLIS = "refresh_interval_in_millis";
        static final String AVAILABLE_PROCESSORS = "available_processors";
        static final String ALLOCATED_PROCESSORS = "allocated_processors";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.timeValueField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, refreshInterval);
        if (name != null) {
            builder.field(Fields.NAME, name);
        }
        if (arch != null) {
            builder.field(Fields.ARCH, arch);
        }
        if (version != null) {
            builder.field(Fields.VERSION, version);
        }
        builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
        builder.field(Fields.ALLOCATED_PROCESSORS, allocatedProcessors);
        builder.endObject();
        return builder;
    }

    public static OsInfo readOsInfo(StreamInput in) throws IOException {
        OsInfo info = new OsInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
        availableProcessors = in.readInt();
        allocatedProcessors = in.readInt();
        name = in.readOptionalString();
        arch = in.readOptionalString();
        version = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        out.writeInt(availableProcessors);
        out.writeInt(allocatedProcessors);
        out.writeOptionalString(name);
        out.writeOptionalString(arch);
        out.writeOptionalString(version);
    }
}
