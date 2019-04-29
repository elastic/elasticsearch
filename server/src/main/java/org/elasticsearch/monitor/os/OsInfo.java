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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class OsInfo implements Writeable, ToXContentFragment {

    private final long refreshInterval;
    private final int availableProcessors;
    private final int allocatedProcessors;
    private final String name;
    private final String prettyName;
    private final String arch;
    private final String version;

    public OsInfo(
            final long refreshInterval,
            final int availableProcessors,
            final int allocatedProcessors,
            final String name,
            final String prettyName,
            final String arch,
            final String version) {
        this.refreshInterval = refreshInterval;
        this.availableProcessors = availableProcessors;
        this.allocatedProcessors = allocatedProcessors;
        this.name = name;
        this.prettyName = prettyName;
        this.arch = arch;
        this.version = version;
    }

    public OsInfo(StreamInput in) throws IOException {
        this.refreshInterval = in.readLong();
        this.availableProcessors = in.readInt();
        this.allocatedProcessors = in.readInt();
        this.name = in.readOptionalString();
        this.prettyName = in.readOptionalString();
        this.arch = in.readOptionalString();
        this.version = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        out.writeInt(availableProcessors);
        out.writeInt(allocatedProcessors);
        out.writeOptionalString(name);
        out.writeOptionalString(prettyName);
        out.writeOptionalString(arch);
        out.writeOptionalString(version);
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

    public String getPrettyName() {
        return prettyName;
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
        static final String PRETTY_NAME = "pretty_name";
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
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, new TimeValue(refreshInterval));
        if (name != null) {
            builder.field(Fields.NAME, name);
        }
        if (prettyName != null) {
            builder.field(Fields.PRETTY_NAME, prettyName);
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
}
