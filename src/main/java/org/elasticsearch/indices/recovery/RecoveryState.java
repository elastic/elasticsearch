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

package org.elasticsearch.indices.recovery;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of state related to shard recovery.
 */
public class RecoveryState implements ToXContent, Streamable {

    public static enum Stage {
        INIT((byte) 0),
        INDEX((byte) 1),
        START((byte) 2),
        TRANSLOG((byte) 3),
        FINALIZE((byte) 4),
        DONE((byte) 5);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= STAGES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    public static enum Type {
        GATEWAY((byte) 0),
        SNAPSHOT((byte) 1),
        REPLICA((byte) 2),
        RELOCATION((byte) 3);

        private static final Type[] TYPES = new Type[Type.values().length];

        static {
            for (Type type : Type.values()) {
                assert type.id() < TYPES.length && type.id() >= 0;
                TYPES[type.id] = type;
            }
        }

        private final byte id;

        Type(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Type fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= TYPES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return TYPES[id];
        }
    }

    private volatile Stage stage = Stage.INIT;

    private volatile Index index = new Index();
    private volatile Translog translog = new Translog();
    private volatile Start start = new Start();
    private volatile Timer timer = new Timer();

    private volatile Type type;
    private volatile ShardId shardId;
    private volatile RestoreSource restoreSource;
    private volatile DiscoveryNode sourceNode;
    private volatile DiscoveryNode targetNode;

    private volatile boolean primary = false;

    public RecoveryState() {
    }

    public RecoveryState(ShardId shardId) {
        this.shardId = shardId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Stage getStage() {
        return this.stage;
    }

    public RecoveryState setStage(Stage stage) {
        this.stage = stage;
        if (stage == Stage.DONE) {
            timer.stopTime(System.currentTimeMillis());
        }
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public Start getStart() {
        return this.start;
    }

    public Translog getTranslog() {
        return translog;
    }

    public Timer getTimer() {
        return timer;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setSourceNode(DiscoveryNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public void setTargetNode(DiscoveryNode targetNode) {
        this.targetNode = targetNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public void setRestoreSource(RestoreSource restoreSource) {
        this.restoreSource = restoreSource;
    }

    public RestoreSource getRestoreSource() {
        return restoreSource;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public boolean getPrimary() {
        return primary;
    }

    public static RecoveryState readRecoveryState(StreamInput in) throws IOException {
        RecoveryState recoveryState = new RecoveryState();
        recoveryState.readFrom(in);
        return recoveryState;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timer.startTime(in.readVLong());
        timer.stopTime(in.readVLong());
        timer.time = in.readVLong();
        type = Type.fromId(in.readByte());
        stage = Stage.fromId(in.readByte());
        shardId = ShardId.readShardId(in);
        restoreSource = RestoreSource.readOptionalRestoreSource(in);
        targetNode = DiscoveryNode.readNode(in);
        if (in.readBoolean()) {
            sourceNode = DiscoveryNode.readNode(in);
        }
        index = Index.readIndex(in);
        translog = Translog.readTranslog(in);
        start = Start.readStart(in);
        if (in.getVersion().before(Version.V_1_5_0)) {
            // used to the detailed flag
            in.readBoolean();
        }
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timer.startTime());
        out.writeVLong(timer.stopTime());
        out.writeVLong(timer.time());
        out.writeByte(type.id());
        out.writeByte(stage.id());
        shardId.writeTo(out);
        out.writeOptionalStreamable(restoreSource);
        targetNode.writeTo(out);
        out.writeBoolean(sourceNode != null);
        if (sourceNode != null) {
            sourceNode.writeTo(out);
        }
        index.writeTo(out);
        translog.writeTo(out);
        start.writeTo(out);
        if (out.getVersion().before(Version.V_1_5_0)) {
            // detailed flag
            out.writeBoolean(true);
        }
        out.writeBoolean(primary);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.ID, shardId.id());
        builder.field(Fields.TYPE, type.toString());
        builder.field(Fields.STAGE, stage.toString());
        builder.field(Fields.PRIMARY, primary);
        builder.dateValueField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, timer.startTime);
        if (timer.stopTime > 0) {
            builder.dateValueField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, timer.stopTime);
        }
        builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, timer.time());

        if (restoreSource != null) {
            builder.field(Fields.SOURCE);
            restoreSource.toXContent(builder, params);
        } else {
            builder.startObject(Fields.SOURCE);
            builder.field(Fields.ID, sourceNode.id());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.address().toString());
            builder.field(Fields.IP, sourceNode.getHostAddress());
            builder.field(Fields.NAME, sourceNode.name());
            builder.endObject();
        }

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.id());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.address().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.name());
        builder.endObject();

        builder.startObject(Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.TRANSLOG);
        translog.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.START);
        start.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString TYPE = new XContentBuilderString("type");
        static final XContentBuilderString STAGE = new XContentBuilderString("stage");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString START_TIME_IN_MILLIS = new XContentBuilderString("start_time_in_millis");
        static final XContentBuilderString STOP_TIME = new XContentBuilderString("stop_time");
        static final XContentBuilderString STOP_TIME_IN_MILLIS = new XContentBuilderString("stop_time_in_millis");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString TOTAL_TIME_IN_MILLIS = new XContentBuilderString("total_time_in_millis");
        static final XContentBuilderString SOURCE = new XContentBuilderString("source");
        static final XContentBuilderString HOST = new XContentBuilderString("host");
        static final XContentBuilderString TRANSPORT_ADDRESS = new XContentBuilderString("transport_address");
        static final XContentBuilderString IP = new XContentBuilderString("ip");
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString TARGET = new XContentBuilderString("target");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString TRANSLOG = new XContentBuilderString("translog");
        static final XContentBuilderString START = new XContentBuilderString("start");
        static final XContentBuilderString RECOVERED = new XContentBuilderString("recovered");
        static final XContentBuilderString CHECK_INDEX_TIME = new XContentBuilderString("check_index_time");
        static final XContentBuilderString CHECK_INDEX_TIME_IN_MILLIS = new XContentBuilderString("check_index_time_in_millis");
        static final XContentBuilderString LENGTH = new XContentBuilderString("length");
        static final XContentBuilderString FILES = new XContentBuilderString("files");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString REUSED = new XContentBuilderString("reused");
        static final XContentBuilderString PERCENT = new XContentBuilderString("percent");
        static final XContentBuilderString DETAILS = new XContentBuilderString("details");
        static final XContentBuilderString BYTES = new XContentBuilderString("bytes");
    }

    public static class Timer {
        private volatile long startTime = 0;
        private volatile long time = 0;
        private volatile long stopTime = 0;

        public long startTime() {
            return startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            if (startTime == 0) {
                return 0;
            }
            if (time > 0) {
                return time;
            }
            return Math.max(0, System.currentTimeMillis() - startTime);
        }

        public long stopTime() {
            return stopTime;
        }

        public void stopTime(long stopTime) {
            this.time = Math.max(0, stopTime - startTime);
            this.stopTime = stopTime;
        }
    }

    public static class Start implements ToXContent, Streamable {
        private volatile long startTime;
        private volatile long time;
        private volatile long checkIndexTime;

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return this.time;
        }

        public void time(long time) {
            this.time = time;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        public static Start readStart(StreamInput in) throws IOException {
            Start start = new Start();
            start.readFrom(in);
            return start;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            startTime = in.readVLong();
            time = in.readVLong();
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(time);
            out.writeVLong(checkIndexTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.timeValueField(Fields.CHECK_INDEX_TIME_IN_MILLIS, Fields.CHECK_INDEX_TIME, checkIndexTime);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time);
            return builder;
        }
    }

    public static class Translog implements ToXContent, Streamable {
        private volatile long startTime = 0;
        private volatile long time;
        private final AtomicInteger currentTranslogOperations = new AtomicInteger();

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return this.time;
        }

        public void time(long time) {
            this.time = time;
        }

        public void addTranslogOperations(int count) {
            this.currentTranslogOperations.addAndGet(count);
        }

        public void incrementTranslogOperations() {
            this.currentTranslogOperations.incrementAndGet();
        }

        public int currentTranslogOperations() {
            return this.currentTranslogOperations.get();
        }

        public static Translog readTranslog(StreamInput in) throws IOException {
            Translog translog = new Translog();
            translog.readFrom(in);
            return translog;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            startTime = in.readVLong();
            time = in.readVLong();
            currentTranslogOperations.set(in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(time);
            out.writeVInt(currentTranslogOperations.get());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, currentTranslogOperations);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time);
            return builder;
        }
    }

    public static class File implements ToXContent, Streamable {
        private String name;
        private long length;
        private long recovered;
        private boolean reused;

        public File() {
        }

        public File(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered += bytes;
        }

        /** file name * */
        public String name() {
            return name;
        }

        /** file length * */
        public long length() {
            return length;
        }

        /** number of bytes recovered for this file (so far). 0 if the file is reused * */
        public long recovered() {
            return recovered;
        }

        /** returns true if the file is reused from a local copy */
        public boolean reused() {
            return reused;
        }

        boolean fullyRecovered() {
            return reused == false && length == recovered;
        }

        public static File readFile(StreamInput in) throws IOException {
            File file = new File();
            file.readFrom(in);
            return file;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            recovered = in.readVLong();
            if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
                reused = in.readBoolean();
            } else {
                reused = recovered > 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
            if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
                out.writeBoolean(reused);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.field(Fields.LENGTH, length);
            builder.field(Fields.REUSED, reused);
            builder.field(Fields.RECOVERED, recovered);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof File) {
                return name.equals(((File) obj).name);
            }
            return false;
        }
    }

    public static class Index implements ToXContent, Streamable {

        private volatile long startTime = 0;
        private volatile long time = 0;

        private Set<File> fileDetails = ConcurrentCollections.newConcurrentSet();

        private volatile long version = -1;

        public Set<File> fileDetails() {
            return ImmutableSet.copyOf(fileDetails);
        }

        public void addFileDetail(String name, long length, boolean reused) {
            File file = new File(name, length, reused);
            boolean exists = fileDetails.add(file);
            assert exists == false : "file [" + name + "] is already reported";
        }

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return this.time;
        }

        public void time(long time) {
            assert time >= 0;
            this.time = time;
        }

        public long version() {
            return this.version;
        }

        /** total number of files that are part of this recovery, both re-used and recovered */
        public int totalFileCount() {
            return fileDetails.size();
        }

        /** number of file that were recovered (excluding on ongoing files) */
        public int recoveredFileCount() {
            int count = 0;
            for (File file : fileDetails) {
                if (file.fullyRecovered()) {
                    count++;
                }
            }
            return count;
        }

        /** percent of recovered (i.e., not reused) files out of the total files to be recovered */
        public float recoverdFilesPercent() {
            int total = 0;
            int recovered = 0;
            for (File file : fileDetails) {
                if (file.reused() == false) {
                    total++;
                    if (file.fullyRecovered()) {
                        recovered++;
                    }
                }
            }
            if (total == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if ((total - recovered) == 0) {
                return 100.0f;
            } else {
                float result = 100.0f * (recovered / (float) total);
                return result;
            }
        }

        /** total number of bytes in th shard */
        public long totalBytes() {
            long total = 0;
            for (File file : fileDetails) {
                total += file.length();
            }
            return total;
        }

        /** total number of bytes recovered so far, including both existing and reused */
        public long recoveredBytes() {
            long recovered = 0;
            for (File file : fileDetails) {
                recovered += file.recovered();
            }
            return recovered;
        }

        /** total bytes of files to be recovered (potentially not yet done) */
        public long totalRecoverBytes() {
            long total = 0;
            for (File file : fileDetails) {
                if (file.reused() == false) {
                    total += file.length();
                }
            }
            return total;
        }

        public long totalReuseBytes() {
            long total = 0;
            for (File file : fileDetails) {
                if (file.reused()) {
                    total += file.length();
                }
            }
            return total;
        }

        /** percent of bytes recovered out of total files bytes *to be* recovered */
        public float recoveredBytesPercent() {
            long total = 0;
            long recovered = 0;
            for (File file : fileDetails) {
                if (file.reused() == false) {
                    total += file.length();
                    recovered += file.recovered();
                }
            }
            if (total == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if ((total - recovered) == 0) {
                return 100.0f;
            } else {
                float result = 100.0f * (recovered / (float) total);
                return result;
            }
        }

        public int reusedFileCount() {
            int reused = 0;
            for (File file : fileDetails) {
                if (file.reused()) {
                    reused++;
                }
            }
            return reused;
        }

        public long reusedBytes() {
            long reused = 0;
            for (File file : fileDetails) {
                if (file.reused()) {
                    reused += file.length();
                }
            }
            return reused;
        }

        public void updateVersion(long version) {
            this.version = version;
        }

        public static Index readIndex(StreamInput in) throws IOException {
            Index index = new Index();
            index.readFrom(in);
            return index;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            startTime = in.readVLong();
            time = in.readVLong();
            if (in.getVersion().before(Version.V_1_5_0)) {
                // This may result in skewed reports as we didn't report all files in advance, relying on this totals
                in.readVInt(); // totalFileCount
                in.readVLong(); // totalBytes
                in.readVInt(); // reusedFileCount
                in.readVLong(); // reusedByteCount
                in.readVInt(); // recoveredFileCount
                in.readVLong(); // recoveredByteCount
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    fileDetails.add(File.readFile(in));
                }
                size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    fileDetails.add(File.readFile(in));
                }
            } else {
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    fileDetails.add(File.readFile(in));
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(time);
            if (out.getVersion().before(Version.V_1_5_0)) {
                out.writeVInt(totalFileCount());
                out.writeVLong(totalBytes());
                out.writeVInt(reusedFileCount());
                out.writeVLong(reusedBytes());
                out.writeVInt(recoveredFileCount());
                out.writeVLong(recoveredBytes());
                final File[] files = fileDetails.toArray(new File[0]);
                int nonReusedCount = 0;
                int reusedCount = 0;
                for (File file : files) {
                    if (file.reused()) {
                        reusedCount++;
                    } else {
                        nonReusedCount++;
                    }
                }
                out.writeVInt(nonReusedCount);
                for (File file : fileDetails) {
                    if (file.reused() == false) {
                        file.writeTo(out);
                    }
                }
                out.writeVInt(reusedCount);
                for (File file : fileDetails) {
                    if (file.reused()) {
                        file.writeTo(out);
                    }
                }
            } else {
                final File[] files = fileDetails.toArray(new File[0]);
                out.writeVInt(files.length);
                for (File file : fileDetails) {
                    file.writeTo(out);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

            builder.startObject(Fields.FILES);
            builder.field(Fields.TOTAL, totalFileCount());
            builder.field(Fields.REUSED, reusedFileCount());
            builder.field(Fields.RECOVERED, recoveredFileCount());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoverdFilesPercent()));
            if (params.paramAsBoolean("details", false)) {
                builder.startArray(Fields.DETAILS);
                for (File file : fileDetails) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject(Fields.BYTES);
            builder.field(Fields.TOTAL, totalBytes());
            builder.field(Fields.REUSED, totalReuseBytes());
            builder.field(Fields.RECOVERED, recoveredBytes());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
            builder.endObject();
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time);

            return builder;
        }

        @Override
        public String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (IOException e) {
                return "{ \"error\" : \"" + e.getMessage() + "\"}";
            }
        }

    }
}
