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

import org.elasticsearch.common.xcontent.XContentBuilderString;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Locale;

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

    private Index index = new Index();
    private Translog translog = new Translog();
    private Start start = new Start();
    private Timer timer = new Timer();

    private Type type;
    private ShardId shardId;
    private RestoreSource restoreSource;
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;

    private boolean detailed = false;
    private boolean primary = false;

    public RecoveryState() { }

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

    public void setDetailed(boolean detailed) {
        this.detailed = detailed;
        this.index.detailed(detailed);
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
        timer.time(in.readVLong());
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
        detailed = in.readBoolean();
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
        out.writeBoolean(detailed);
        out.writeBoolean(primary);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.ID, shardId.id());
        builder.field(Fields.TYPE, type.toString());
        builder.field(Fields.STAGE, stage.toString());
        builder.field(Fields.PRIMARY, primary);
        builder.timeValueField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, timer.startTime);
        builder.timeValueField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, timer.stopTime);
        builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, timer.time);

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
        index.detailed(this.detailed);
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
        private long startTime = 0;
        private long time = 0;
        private long stopTime = 0;

        public long startTime() {
            return startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return time;
        }

        public void time(long time) {
            this.time = time;
        }

        public long stopTime() {
            return stopTime;
        }

        public void stopTime(long stopTime) {
            this.stopTime = stopTime;
        }
    }

    public static class Start implements ToXContent, Streamable {
        private long startTime;
        private long time;
        private long checkIndexTime;

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
        private long startTime = 0;
        private long time;
        private volatile int currentTranslogOperations = 0;

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
            this.currentTranslogOperations += count;
        }

        public void incrementTranslogOperations() {
            this.currentTranslogOperations++;
        }

        public int currentTranslogOperations() {
            return this.currentTranslogOperations;
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
            currentTranslogOperations = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(time);
            out.writeVInt(currentTranslogOperations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, currentTranslogOperations);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time);
            return builder;
        }
    }

    public static class File implements ToXContent, Streamable {
        String name;
        long length;
        long recovered;

        public File() { }

        public File(String name, long length) {
            this.name = name;
            this.length = length;
        }

        public void updateRecovered(long length) {
            recovered += length;
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.field(Fields.LENGTH, length);
            builder.field(Fields.RECOVERED, recovered);
            builder.endObject();
            return builder;
        }
    }

    public static class Index implements ToXContent, Streamable {

        private long startTime = 0;
        private long time = 0;

        private List<File> fileDetails = new ArrayList<>();
        private List<File> reusedFileDetails = new ArrayList<>();

        private long version = -1;

        private boolean detailed = false;

        private int totalFileCount = 0;
        private int reusedFileCount = 0;
        private AtomicInteger recoveredFileCount = new AtomicInteger();

        private long totalByteCount = 0;
        private long reusedByteCount = 0;
        private AtomicLong recoveredByteCount = new AtomicLong();

        public List<File> fileDetails() {
            return fileDetails;
        }
        public List<File> reusedFileDetails() {
            return reusedFileDetails;
        }

        public void addFileDetail(String name, long length) {
            fileDetails.add(new File(name, length));
        }

        public void addFileDetail(String name, long length, long recovered) {
            File file = new File(name, length);
            file.recovered = recovered;
            fileDetails.add(file);
        }

        public void addFileDetails(List<String> names, List<Long> lengths) {
            for (int i = 0; i < names.size(); i++) {
                fileDetails.add(new File(names.get(i), lengths.get(i)));
            }
        }

        public void addReusedFileDetail(String name, long length) {
            reusedFileDetails.add(new File(name, length));
        }

        public void addReusedFileDetails(List<String> names, List<Long> lengths) {
            for (int i = 0; i < names.size(); i++) {
                reusedFileDetails.add(new File(names.get(i), lengths.get(i)));
            }
        }

        public File file(String name) {
            for (File file : fileDetails) {
                if (file.name.equals(name))
                    return file;
            }
            for (File file : reusedFileDetails) {
                if (file.name.equals(name)) {
                    return file;
                }
            }
            return null;
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
            this.time = time;
        }

        public long version() {
            return this.version;
        }

        public void files(int totalFileCount, long totalByteCount, int reusedFileCount, long reusedByteCount) {
            this.totalFileCount = totalFileCount;
            this.totalByteCount = totalByteCount;
            this.reusedFileCount = reusedFileCount;
            this.reusedByteCount = reusedByteCount;
        }

        public int totalFileCount() {
            return totalFileCount;
        }

        public void totalFileCount(int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }

        public int recoveredFileCount() {
            return recoveredFileCount.get();
        }

        public void recoveredFileCount(int recoveredFileCount) {
            this.recoveredFileCount.set(recoveredFileCount);
        }

        public void addRecoveredFileCount(int updatedCount) {
            this.recoveredFileCount.addAndGet(updatedCount);
        }

        public float percentFilesRecovered(int numberRecovered) {
            if (totalFileCount == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if ((totalFileCount - reusedFileCount) == 0) {
                return 100.0f;
            } else {
                int d = totalFileCount - reusedFileCount;
                float result = 100.0f * (numberRecovered / (float) d);
                return result;
            }
        }

        public float percentFilesRecovered() {
            return percentFilesRecovered(recoveredFileCount.get());
        }

        public int numberOfRecoveredFiles() {
            return totalFileCount - reusedFileCount;
        }

        public long totalByteCount() {
            return this.totalByteCount;
        }

        public void totalByteCount(long totalByteCount) {
            this.totalByteCount = totalByteCount;
        }

        public long recoveredByteCount() {
            return recoveredByteCount.longValue();
        }

        public void recoveredByteCount(long recoveredByteCount) {
            this.recoveredByteCount.set(recoveredByteCount);
        }

        public void addRecoveredByteCount(long updatedSize) {
            recoveredByteCount.addAndGet(updatedSize);
        }

        public long numberOfRecoveredBytes() {
            return recoveredByteCount.get() - reusedByteCount;
        }

        public float percentBytesRecovered(long numberRecovered) {
            if (totalByteCount == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if ((totalByteCount - reusedByteCount) == 0) {
                return 100.0f;
            } else {
                long d = totalByteCount - reusedByteCount;
                float result = 100.0f * (numberRecovered / (float) d);
                return result;
            }
        }

        public float percentBytesRecovered() {
            return percentBytesRecovered(recoveredByteCount.get());
        }

        public int reusedFileCount() {
            return reusedFileCount;
        }

        public long reusedByteCount() {
            return this.reusedByteCount;
        }

        public void reusedByteCount(long reusedByteCount) {
            this.reusedByteCount = reusedByteCount;
        }

        public long recoveredTotalSize() {
            return totalByteCount - reusedByteCount;
        }

        public void updateVersion(long version) {
            this.version = version;
        }

        public void detailed(boolean detailed) {
            this.detailed = detailed;
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
            totalFileCount = in.readVInt();
            totalByteCount = in.readVLong();
            reusedFileCount = in.readVInt();
            reusedByteCount = in.readVLong();
            recoveredFileCount = new AtomicInteger(in.readVInt());
            recoveredByteCount = new AtomicLong(in.readVLong());
            int size = in.readVInt();
            fileDetails = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fileDetails.add(File.readFile(in));
            }
            size = in.readVInt();
            reusedFileDetails = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                reusedFileDetails.add(File.readFile(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(time);
            out.writeVInt(totalFileCount);
            out.writeVLong(totalByteCount);
            out.writeVInt(reusedFileCount);
            out.writeVLong(reusedByteCount);
            out.writeVInt(recoveredFileCount.get());
            out.writeVLong(recoveredByteCount.get());
            out.writeVInt(fileDetails.size());
            for (File file : fileDetails) {
                file.writeTo(out);
            }
            out.writeVInt(reusedFileDetails.size());
            for (File file : reusedFileDetails) {
                file.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

            int filesRecovered = recoveredFileCount.get();
            long bytesRecovered = recoveredByteCount.get();

            builder.startObject(Fields.FILES);
            builder.field(Fields.TOTAL, totalFileCount);
            builder.field(Fields.REUSED, reusedFileCount);
            builder.field(Fields.RECOVERED, filesRecovered);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", percentFilesRecovered(filesRecovered)));
            if (detailed) {
                builder.startArray(Fields.DETAILS);
                for (File file : fileDetails) {
                    file.toXContent(builder, params);
                }
                for (File file : reusedFileDetails) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject(Fields.BYTES);
            builder.field(Fields.TOTAL, totalByteCount);
            builder.field(Fields.REUSED, reusedByteCount);
            builder.field(Fields.RECOVERED, bytesRecovered);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", percentBytesRecovered(bytesRecovered)));
            builder.endObject();
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time);

            return builder;
        }
    }
}
