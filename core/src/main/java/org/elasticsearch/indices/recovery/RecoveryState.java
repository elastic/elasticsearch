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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Keeps track of state related to shard recovery.
 */
public class RecoveryState implements ToXContent, Streamable {

    public static enum Stage {
        INIT((byte) 0),

        /**
         * recovery of lucene files, either reusing local ones are copying new ones
         */
        INDEX((byte) 1),

        /**
         * potentially running check index
         */
        VERIFY_INDEX((byte) 2),

        /**
         * starting up the engine, replaying the translog
         */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
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

        public static Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    public enum Type {
        STORE((byte) 0),
        SNAPSHOT((byte) 1),
        REPLICA((byte) 2),
        PRIMARY_RELOCATION((byte) 3),
        LOCAL_SHARDS((byte) 4);

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

        public static Type fromId(byte id) {
            if (id < 0 || id >= TYPES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return TYPES[id];
        }
    }

    private Stage stage;

    private final Index index = new Index();
    private final Translog translog = new Translog();
    private final VerifyIndex verifyIndex = new VerifyIndex();
    private final Timer timer = new Timer();

    private Type type;
    private ShardId shardId;
    private RestoreSource restoreSource;
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;
    private boolean primary = false;

    private RecoveryState() {
    }

    public RecoveryState(ShardId shardId, boolean primary, Type type, DiscoveryNode sourceNode, DiscoveryNode targetNode) {
        this(shardId, primary, type, sourceNode, null, targetNode);
    }

    public RecoveryState(ShardId shardId, boolean primary, Type type, RestoreSource restoreSource, DiscoveryNode targetNode) {
        this(shardId, primary, type, null, restoreSource, targetNode);
    }

    private RecoveryState(ShardId shardId, boolean primary, Type type, @Nullable DiscoveryNode sourceNode, @Nullable RestoreSource restoreSource, DiscoveryNode targetNode) {
        this.shardId = shardId;
        this.primary = primary;
        this.type = type;
        this.sourceNode = sourceNode;
        this.restoreSource = restoreSource;
        this.targetNode = targetNode;
        stage = Stage.INIT;
        timer.start();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }


    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            throw new IllegalStateException("can't move recovery to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized RecoveryState setStage(Stage stage) {
        switch (stage) {
            case INIT:
                // reinitializing stop remove all state except for start time
                this.stage = Stage.INIT;
                getIndex().reset();
                getVerifyIndex().reset();
                getTranslog().reset();
                break;
            case INDEX:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case VERIFY_INDEX:
                validateAndSetStage(Stage.INDEX, stage);
                getIndex().stop();
                getVerifyIndex().start();
                break;
            case TRANSLOG:
                validateAndSetStage(Stage.VERIFY_INDEX, stage);
                getVerifyIndex().stop();
                getTranslog().start();
                break;
            case FINALIZE:
                validateAndSetStage(Stage.TRANSLOG, stage);
                getTranslog().stop();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE, stage);
                getTimer().stop();
                break;
            default:
                throw new IllegalArgumentException("unknown RecoveryState.Stage [" + stage + "]");
        }
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public VerifyIndex getVerifyIndex() {
        return this.verifyIndex;
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

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public RestoreSource getRestoreSource() {
        return restoreSource;
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
    public synchronized void readFrom(StreamInput in) throws IOException {
        timer.readFrom(in);
        type = Type.fromId(in.readByte());
        stage = Stage.fromId(in.readByte());
        shardId = ShardId.readShardId(in);
        restoreSource = RestoreSource.readOptionalRestoreSource(in);
        targetNode = new DiscoveryNode(in);
        if (in.readBoolean()) {
            sourceNode = new DiscoveryNode(in);
        }
        index.readFrom(in);
        translog.readFrom(in);
        verifyIndex.readFrom(in);
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
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
        verifyIndex.writeTo(out);
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
            builder.field(Fields.ID, sourceNode.getId());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.getAddress().toString());
            builder.field(Fields.IP, sourceNode.getHostAddress());
            builder.field(Fields.NAME, sourceNode.getName());
            builder.endObject();
        }

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.getId());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.getAddress().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.getName());
        builder.endObject();

        builder.startObject(Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.TRANSLOG);
        translog.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.VERIFY_INDEX);
        verifyIndex.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final String ID = "id";
        static final String TYPE = "type";
        static final String STAGE = "stage";
        static final String PRIMARY = "primary";
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String STOP_TIME = "stop_time";
        static final String STOP_TIME_IN_MILLIS = "stop_time_in_millis";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String SOURCE = "source";
        static final String HOST = "host";
        static final String TRANSPORT_ADDRESS = "transport_address";
        static final String IP = "ip";
        static final String NAME = "name";
        static final String TARGET = "target";
        static final String INDEX = "index";
        static final String TRANSLOG = "translog";
        static final String TOTAL_ON_START = "total_on_start";
        static final String VERIFY_INDEX = "verify_index";
        static final String RECOVERED = "recovered";
        static final String RECOVERED_IN_BYTES = "recovered_in_bytes";
        static final String CHECK_INDEX_TIME = "check_index_time";
        static final String CHECK_INDEX_TIME_IN_MILLIS = "check_index_time_in_millis";
        static final String LENGTH = "length";
        static final String LENGTH_IN_BYTES = "length_in_bytes";
        static final String FILES = "files";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";
        static final String REUSED = "reused";
        static final String REUSED_IN_BYTES = "reused_in_bytes";
        static final String PERCENT = "percent";
        static final String DETAILS = "details";
        static final String SIZE = "size";
        static final String SOURCE_THROTTLE_TIME = "source_throttle_time";
        static final String SOURCE_THROTTLE_TIME_IN_MILLIS = "source_throttle_time_in_millis";
        static final String TARGET_THROTTLE_TIME = "target_throttle_time";
        static final String TARGET_THROTTLE_TIME_IN_MILLIS = "target_throttle_time_in_millis";
    }

    public static class Timer implements Streamable {
        protected long startTime = 0;
        protected long startNanoTime = 0;
        protected long time = -1;
        protected long stopTime = 0;

        public synchronized void start() {
            assert startTime == 0 : "already started";
            startTime = System.currentTimeMillis();
            startNanoTime = System.nanoTime();
        }

        /** Returns start time in millis */
        public synchronized long startTime() {
            return startTime;
        }

        /** Returns elapsed time in millis, or 0 if timer was not started */
        public synchronized long time() {
            if (startNanoTime == 0) {
                return 0;
            }
            if (time >= 0) {
                return time;
            }
            return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
        }

        /** Returns stop time in millis */
        public synchronized long stopTime() {
            return stopTime;
        }

        public synchronized void stop() {
            assert stopTime == 0 : "already stopped";
            stopTime = Math.max(System.currentTimeMillis(), startTime);
            time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
            assert time >= 0;
        }

        public synchronized void reset() {
            startTime = 0;
            startNanoTime = 0;
            time = -1;
            stopTime = 0;
        }


        @Override
        public synchronized void readFrom(StreamInput in) throws IOException {
            startTime = in.readVLong();
            startNanoTime = in.readVLong();
            stopTime = in.readVLong();
            time = in.readVLong();
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(startNanoTime);
            out.writeVLong(stopTime);
            // write a snapshot of current time, which is not per se the time field
            out.writeVLong(time());
        }

    }

    public static class VerifyIndex extends Timer implements ToXContent, Streamable {
        private volatile long checkIndexTime;


        public void reset() {
            super.reset();
            checkIndexTime = 0;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(checkIndexTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.timeValueField(Fields.CHECK_INDEX_TIME_IN_MILLIS, Fields.CHECK_INDEX_TIME, checkIndexTime);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
            return builder;
        }
    }

    public static class Translog extends Timer implements ToXContent, Streamable {
        public static final int UNKNOWN = -1;

        private int recovered;
        private int total = UNKNOWN;
        private int totalOnStart = UNKNOWN;

        public synchronized void reset() {
            super.reset();
            recovered = 0;
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
        }

        public synchronized void incrementRecoveredOperations() {
            recovered++;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        public synchronized void decrementRecoveredOperations(int ops) {
            recovered -= ops;
            assert recovered >= 0 : "recovered operations must be non-negative. Because [" + recovered + "] after decrementing [" + ops + "]";
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }


        /**
         * returns the total number of translog operations recovered so far
         */
        public synchronized int recoveredOperations() {
            return recovered;
        }

        /**
         * returns the total number of translog operations needed to be recovered at this moment.
         * Note that this can change as the number of operations grows during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperations() {
            return total;
        }

        public synchronized void totalOperations(int total) {
            this.total = total;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        /**
         * returns the total number of translog operations to recovered, on the start of the recovery. Unlike {@link #totalOperations}
         * this does change during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperationsOnStart() {
            return this.totalOnStart;
        }

        public synchronized void totalOperationsOnStart(int total) {
            this.totalOnStart = total;
        }

        public synchronized float recoveredPercent() {
            if (total == UNKNOWN) {
                return -1.f;
            }
            if (total == 0) {
                return 100.f;
            }
            return recovered * 100.0f / total;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            recovered = in.readVInt();
            total = in.readVInt();
            totalOnStart = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(recovered);
            out.writeVInt(total);
            out.writeVInt(totalOnStart);
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, recovered);
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredPercent()));
            builder.field(Fields.TOTAL_ON_START, totalOnStart);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
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

        /**
         * file name *
         */
        public String name() {
            return name;
        }

        /**
         * file length *
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused *
         */
        public long recovered() {
            return recovered;
        }

        /**
         * returns true if the file is reused from a local copy
         */
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
            reused = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
            out.writeBoolean(reused);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.byteSizeField(Fields.LENGTH_IN_BYTES, Fields.LENGTH, length);
            builder.field(Fields.REUSED, reused);
            builder.byteSizeField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, recovered);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof File) {
                File other = (File) obj;
                return name.equals(other.name) && length == other.length() && reused == other.reused() && recovered == other.recovered();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Long.hashCode(length);
            result = 31 * result + Long.hashCode(recovered);
            result = 31 * result + (reused ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "file (name [" + name + "], reused [" + reused + "], length [" + length + "], recovered [" + recovered + "])";
        }
    }

    public static class Index extends Timer implements ToXContent, Streamable {

        private Map<String, File> fileDetails = new HashMap<>();

        public final static long UNKNOWN = -1L;

        private long version = UNKNOWN;
        private long sourceThrottlingInNanos = UNKNOWN;
        private long targetThrottleTimeInNanos = UNKNOWN;

        public synchronized List<File> fileDetails() {
            return Collections.unmodifiableList(new ArrayList<>(fileDetails.values()));
        }

        public synchronized void reset() {
            super.reset();
            version = UNKNOWN;
            fileDetails.clear();
            sourceThrottlingInNanos = UNKNOWN;
            targetThrottleTimeInNanos = UNKNOWN;
        }

        public synchronized void addFileDetail(String name, long length, boolean reused) {
            File file = new File(name, length, reused);
            File existing = fileDetails.put(name, file);
            assert existing == null : "file [" + name + "] is already reported";
        }

        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            File file = fileDetails.get(name);
            file.addRecoveredBytes(bytes);
        }

        public synchronized long version() {
            return this.version;
        }

        public synchronized void addSourceThrottling(long timeInNanos) {
            if (sourceThrottlingInNanos == UNKNOWN) {
                sourceThrottlingInNanos = timeInNanos;
            } else {
                sourceThrottlingInNanos += timeInNanos;
            }
        }

        public synchronized void addTargetThrottling(long timeInNanos) {
            if (targetThrottleTimeInNanos == UNKNOWN) {
                targetThrottleTimeInNanos = timeInNanos;
            } else {
                targetThrottleTimeInNanos += timeInNanos;
            }
        }

        public synchronized TimeValue sourceThrottling() {
            return TimeValue.timeValueNanos(sourceThrottlingInNanos);
        }

        public synchronized TimeValue targetThrottling() {
            return TimeValue.timeValueNanos(targetThrottleTimeInNanos);
        }

        /**
         * total number of files that are part of this recovery, both re-used and recovered
         */
        public synchronized int totalFileCount() {
            return fileDetails.size();
        }

        /**
         * total number of files to be recovered (potentially not yet done)
         */
        public synchronized int totalRecoverFiles() {
            int total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                }
            }
            return total;
        }


        /**
         * number of file that were recovered (excluding on ongoing files)
         */
        public synchronized int recoveredFileCount() {
            int count = 0;
            for (File file : fileDetails.values()) {
                if (file.fullyRecovered()) {
                    count++;
                }
            }
            return count;
        }

        /**
         * percent of recovered (i.e., not reused) files out of the total files to be recovered
         */
        public synchronized float recoveredFilesPercent() {
            int total = 0;
            int recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                    if (file.fullyRecovered()) {
                        recovered++;
                    }
                }
            }
            if (total == 0 && fileDetails.size() == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                float result = 100.0f * (recovered / (float) total);
                return result;
            }
        }

        /**
         * total number of bytes in th shard
         */
        public synchronized long totalBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                total += file.length();
            }
            return total;
        }

        /**
         * total number of bytes recovered so far, including both existing and reused
         */
        public synchronized long recoveredBytes() {
            long recovered = 0;
            for (File file : fileDetails.values()) {
                recovered += file.recovered();
            }
            return recovered;
        }

        /**
         * total bytes of files to be recovered (potentially not yet done)
         */
        public synchronized long totalRecoverBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                }
            }
            return total;
        }

        public synchronized long totalReuseBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    total += file.length();
                }
            }
            return total;
        }

        /**
         * percent of bytes recovered out of total files bytes *to be* recovered
         */
        public synchronized float recoveredBytesPercent() {
            long total = 0;
            long recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                    recovered += file.recovered();
                }
            }
            if (total == 0 && fileDetails.size() == 0) {
                // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                return 100.0f * recovered / total;
            }
        }

        public synchronized int reusedFileCount() {
            int reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused++;
                }
            }
            return reused;
        }

        public synchronized long reusedBytes() {
            long reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused += file.length();
                }
            }
            return reused;
        }

        public synchronized void updateVersion(long version) {
            this.version = version;
        }

        @Override
        public synchronized void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                File file = File.readFile(in);
                fileDetails.put(file.name, file);
            }
            sourceThrottlingInNanos = in.readLong();
            targetThrottleTimeInNanos = in.readLong();
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            final File[] files = fileDetails.values().toArray(new File[0]);
            out.writeVInt(files.length);
            for (File file : files) {
                file.writeTo(out);
            }
            out.writeLong(sourceThrottlingInNanos);
            out.writeLong(targetThrottleTimeInNanos);
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // stream size first, as it matters more and the files section can be long
            builder.startObject(Fields.SIZE);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, totalBytes());
            builder.byteSizeField(Fields.REUSED_IN_BYTES, Fields.REUSED, reusedBytes());
            builder.byteSizeField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, recoveredBytes());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
            builder.endObject();

            builder.startObject(Fields.FILES);
            builder.field(Fields.TOTAL, totalFileCount());
            builder.field(Fields.REUSED, reusedFileCount());
            builder.field(Fields.RECOVERED, recoveredFileCount());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredFilesPercent()));
            if (params.paramAsBoolean("details", false)) {
                builder.startArray(Fields.DETAILS);
                for (File file : fileDetails.values()) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
            builder.timeValueField(Fields.SOURCE_THROTTLE_TIME_IN_MILLIS, Fields.SOURCE_THROTTLE_TIME, sourceThrottling());
            builder.timeValueField(Fields.TARGET_THROTTLE_TIME_IN_MILLIS, Fields.TARGET_THROTTLE_TIME, targetThrottling());
            return builder;
        }

        @Override
        public synchronized String toString() {
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

        public File getFileDetails(String dest) {
            return fileDetails.get(dest);
        }
    }
}
