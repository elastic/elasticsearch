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

package org.elasticsearch.monitor.fs;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class FsInfo implements Iterable<FsInfo.Path>, Streamable, ToXContent {

    public static class Path implements Streamable, ToXContent {

        String path;
        @Nullable
        String mount;
        /** File system type from {@code java.nio.file.FileStore type()}, if available. */
        @Nullable
        String type;
        long total = -1;
        long free = -1;
        long available = -1;

        /** Uses Lucene's {@code IOUtils.spins} method to try to determine if the device backed by spinning media.
         *  This is null if we could not determine it, true if it possibly spins, else false. */
        Boolean spins = null;

        public Path() {
        }

        public Path(String path, @Nullable String mount, long total, long free, long available) {
            this.path = path;
            this.mount = mount;
            this.total = total;
            this.free = free;
            this.available = available;
        }

        static public Path readInfoFrom(StreamInput in) throws IOException {
            Path i = new Path();
            i.readFrom(in);
            return i;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            path = in.readOptionalString();
            mount = in.readOptionalString();
            type = in.readOptionalString();
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
            spins = in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(path); // total aggregates do not have a path
            out.writeOptionalString(mount);
            out.writeOptionalString(type);
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
            out.writeOptionalBoolean(spins);
        }

        public String getPath() {
            return path;
        }

        public String getMount() {
            return mount;
        }

        public String getType() {
            return type;
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getAvailable() {
            return new ByteSizeValue(available);
        }

        public Boolean getSpins() {
            return spins;
        }

        private long addLong(long current, long other) {
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        private double addDouble(double current, double other) {
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        public void add(Path path) {
            total = addLong(total, path.total);
            free = addLong(free, path.free);
            available = addLong(available, path.available);
            if (path.spins != null && path.spins.booleanValue()) {
                // Spinning is contagious!
                spins = Boolean.TRUE;
            }
        }

        static final class Fields {
            static final XContentBuilderString PATH = new XContentBuilderString("path");
            static final XContentBuilderString MOUNT = new XContentBuilderString("mount");
            static final XContentBuilderString TYPE = new XContentBuilderString("type");
            static final XContentBuilderString TOTAL = new XContentBuilderString("total");
            static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");
            static final XContentBuilderString FREE = new XContentBuilderString("free");
            static final XContentBuilderString FREE_IN_BYTES = new XContentBuilderString("free_in_bytes");
            static final XContentBuilderString AVAILABLE = new XContentBuilderString("available");
            static final XContentBuilderString AVAILABLE_IN_BYTES = new XContentBuilderString("available_in_bytes");
            static final XContentBuilderString SPINS = new XContentBuilderString("spins");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (path != null) {
                builder.field(Fields.PATH, path, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (mount != null) {
                builder.field(Fields.MOUNT, mount, XContentBuilder.FieldCaseConversion.NONE);
            }
            if (type != null) {
                builder.field(Fields.TYPE, type, XContentBuilder.FieldCaseConversion.NONE);
            }

            if (total != -1) {
                builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, total);
            }
            if (free != -1) {
                builder.byteSizeField(Fields.FREE_IN_BYTES, Fields.FREE, free);
            }
            if (available != -1) {
                builder.byteSizeField(Fields.AVAILABLE_IN_BYTES, Fields.AVAILABLE, available);
            }
            if (spins != null) {
                builder.field(Fields.SPINS, spins.toString());
            }

            builder.endObject();
            return builder;
        }
    }

    long timestamp;
    Path total;
    Path[] paths;

    FsInfo() {

    }

    public FsInfo(long timestamp, Path[] paths) {
        this.timestamp = timestamp;
        this.paths = paths;
        this.total = null;
    }

    public Path getTotal() {
        return total();
    }

    public Path total() {
        if (total != null) {
            return total;
        }
        Path res = new Path();
        Set<String> seenDevices = new HashSet<>(paths.length);
        for (Path subPath : paths) {
            if (subPath.path != null) {
                if (!seenDevices.add(subPath.path)) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subPath);
        }
        total = res;
        return res;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Iterator<Path> iterator() {
        return Iterators.forArray(paths);
    }

    public static FsInfo readFsInfo(StreamInput in) throws IOException {
        FsInfo stats = new FsInfo();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        paths = new Path[in.readVInt()];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Path.readInfoFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVInt(paths.length);
        for (Path path : paths) {
            path.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString FS = new XContentBuilderString("fs");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString DATA = new XContentBuilderString("data");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.TOTAL);
        total().toXContent(builder, params);
        builder.startArray(Fields.DATA);
        for (Path path : paths) {
            path.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
