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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;


public class RecoveryMetrics implements ToXContent, Streamable {

    private ShardId shardId;

    private Type type;
    private State state;
    private Index index;
    private Translog translog;

    private Timer timer;
    private Timer indexTimer;
    private Timer translogTimer;

    private boolean failed = false;
    private boolean ignored = false;
    private boolean completed = false;
    private int retries = 0;

    private String exceptionMessage;
    private String ignoredReason;

    // in the case of snapshot restore, sourceNode will be null and
    // restoreSource will have details on the source of the data
    DiscoveryNode sourceNode;
    DiscoveryNode targetNode;
    RestoreSource restoreSource;

    private RecoveryMetrics() { }

    public RecoveryMetrics(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode) {
        this.shardId = shardId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.state = State.INIT;
        this.timer = new Timer();
        this.indexTimer = new Timer();
        this.translogTimer = new Timer();
        this.index = new Index();
        this.translog = new Translog();
        timer.start();
    }

    public RecoveryMetrics(ShardId shardId) {
        this(shardId, null, null);
    }

    public enum Type {
        GATEWAY((byte) 0),
        RESTORE((byte) 1),
        PEER((byte) 2),
        RELOCATION((byte) 3);

        private final byte id;
        private static final Type[] TYPES = new Type[Type.values().length];

        static {
            for (Type type : Type.values()) {
                assert type.id() < TYPES.length && type.id() >= 0;
                TYPES[type.id()] = type;
            }
        }

        Type(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Type fromByte(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= TYPES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return TYPES[id];
        }
    }

    public enum State {
        INIT((byte) 0),
        INDEX((byte) 1),
        TRANSLOG((byte) 2),
        COMPLETE((byte) 3),
        FAILED((byte) 4),
        IGNORED((byte) 5);

        private final byte id;
        private static final State[] STATES = new State[State.values().length];

        static {
            for (State state : State.values()) {
                assert state.id() < STATES.length && state.id() >= 0;
                STATES[state.id()] = state;
            }
        }

        State(byte id) {
            this.id = id;
        }

        public byte id() {
            return this.id;
        }

        public static State fromByte(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= STATES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STATES[id];
        }
    }

    public ShardId shardId() {
        return shardId;
    }

    public Timer timer() {
        return timer;
    }

    public Timer indexTimer() {
        return indexTimer;
    }

    public Timer translogTimer() {
        return translogTimer;
    }

    public void state(State state) {
        this.state = state;

        switch (state) {
            case INDEX:
                indexTimer.start();
                break;
            case TRANSLOG:
                indexTimer.stop();
                translogTimer.start();
                break;
            case COMPLETE:
                completed = true;
                stopTimers();
                break;
            case FAILED:
                failed = true;
                stopTimers();
                break;
            case IGNORED:
                ignored = true;
                stopTimers();
                break;
            default:
                break;
        }
    }

    public void type(Type type) {
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public State state() {
        return state;
    }

    public void sourceNode(DiscoveryNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public DiscoveryNode sourceNode() {
        return sourceNode;
    }

    public void targetNode(DiscoveryNode targetNode) {
        this.targetNode = targetNode;
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    public void restoreSource(RestoreSource restoreSource) {
        this.restoreSource = restoreSource;
    }

    public RestoreSource restoreSource() {
        return restoreSource;
    }

    public void ignored(String reason) {
        this.ignoredReason = reason;
        state(State.IGNORED);
    }

    public boolean ignored() {
        return ignored;
    }

    public void failed(Exception exception) {
        this.exceptionMessage = exception.getMessage();
        state(State.FAILED);
    }

    public boolean failed() {
        return failed;
    }

    public void completed(boolean completed) {
        this.completed = completed;
        if (completed) {
            state(State.COMPLETE);
        }
    }

    public int retries() {
        return retries;
    }

    public void incrementRetries() {
        retries++;
    }

    public boolean completed() {
        return completed;
    }

    public Index index() {
        return index;
    }

    public Translog translog() {
        return translog;
    }

    private void stopTimers() {
        indexTimer.stop();
        translogTimer.stop();
        timer.stop();
    }

    public static RecoveryMetrics readOptionalRecoveryMetricsFrom(StreamInput in) throws IOException {
        return in.readOptionalStreamable(new RecoveryMetrics());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        type = Type.fromByte(in.readByte());
        state = State.fromByte(in.readByte());
        index = Index.readIndexFrom(in);
        translog = Translog.readTranslogFrom(in);
        timer = Timer.readTimerFrom(in);
        indexTimer = Timer.readTimerFrom(in);
        translogTimer = Timer.readTimerFrom(in);
        failed = in.readBoolean();
        ignored = in.readBoolean();
        completed = in.readBoolean();
        retries = in.readVInt();
        exceptionMessage = in.readOptionalString();
        ignoredReason = in.readOptionalString();
        sourceNode = DiscoveryNode.readNode(in);
        targetNode = DiscoveryNode.readNode(in);
        restoreSource = RestoreSource.readOptionalRestoreSource(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeByte(type.id());
        out.writeByte(state.id());
        index.writeTo(out);
        translog.writeTo(out);
        timer.writeTo(out);
        indexTimer.writeTo(out);
        translogTimer.writeTo(out);
        out.writeBoolean(failed);
        out.writeBoolean(ignored);
        out.writeBoolean(completed);
        out.writeVInt(retries);
        out.writeOptionalString(exceptionMessage);
        out.writeOptionalString(ignoredReason);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        out.writeOptionalStreamable(restoreSource);
    }
    
    public static class Index implements Streamable {
        long version = -1;
        int numberOfFiles = 0;
        long totalSize = 0;
        int numberOfReusedFiles = 0;
        long reusedTotalSize = 0;
        long checkIndexTime = 0;

        private AtomicLong numberOfFilesRecovered = new AtomicLong();
        private AtomicLong numberOfBytesRecovered = new AtomicLong();

        Map<String, File> files = new HashMap<String, File>();

        private Index() { }

        public void version(long version) {
            this.version = version;
        }

        public long version() {
            return version;
        }

        public void files(int numberOfFiles, long totalSize, int numberOfReusedFiles, long reusedTotalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
            this.numberOfReusedFiles = numberOfReusedFiles;
            this.reusedTotalSize = reusedTotalSize;
        }

        public long numberOfBytes() {
            return totalSize;
        }

        public long numberOfBytesRecovered() {
            return numberOfBytesRecovered.longValue();
        }

        public void addBytesRecovered(long count) {
            numberOfBytesRecovered.addAndGet(count);
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public long numberOfFilesRecovered() {
            return numberOfFilesRecovered.longValue();
        }

        public void fileCompleted(String fileName) {
            getFile(fileName).complete();
            numberOfFilesRecovered.incrementAndGet();
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        public Map<String, File> files() {
            return files;
        }

        public void addFile(String name, long length) {
            files.put(name, new File(name, length));
        }

        public void addFiles(List<String> names, List<Long> sizes) {
            long totalSize = 0;
            for (int i = 0; i < names.size(); i++) {
                addFile(names.get(i), sizes.get(i));
                totalSize += sizes.get(i);
            }

            this.totalSize = totalSize;
            this.numberOfFiles = names.size();
        }

        public File getFile(String name) {
            return files.get(name);
        }

        public static Index readIndexFrom(StreamInput in) throws IOException {
            Index index = new Index();
            index.readFrom(in);
            return index;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            version = in.readLong();
            numberOfFiles = in.readVInt();
            totalSize = in.readVLong();
            numberOfReusedFiles = in.readVInt();
            reusedTotalSize = in.readVLong();
            checkIndexTime = in.readVLong();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                files.put(in.readString(), File.readFileFrom(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            out.writeVInt(numberOfFiles);
            out.writeVLong(totalSize);
            out.writeVInt(numberOfReusedFiles);
            out.writeVLong(reusedTotalSize);
            out.writeVLong(checkIndexTime);
            out.writeVInt(files == null ? 0 : files.size());
            for (Map.Entry<String, File> entry : files.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    public static class File implements Streamable {
        String name;
        long length;
        long bytesRecovered = 0;
        boolean complete = false;

        private File() { }

        public File(String name, long length) {
            this.name = name;
            this.length = length;
        }

        public void bytesRecovered(long bytesRecovered) {
            this.bytesRecovered += bytesRecovered;
        }

        public long bytesRecovered() {
            return bytesRecovered;
        }

        public void complete() {
            complete = true;
        }

        public static File readFileFrom(StreamInput in) throws IOException {
            File f = new File();
            f.readFrom(in);
            return f;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            bytesRecovered = in.readVLong();
            complete = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(bytesRecovered);
            out.writeBoolean(complete);
        }
    }

    public static class Translog implements Streamable {
        private volatile int currentTranslogOperations = 0;

        private Translog() { }

        public void addTranslogOperations(int count) {
            this.currentTranslogOperations += count;
        }

        public int currentTranslogOperations() {
            return this.currentTranslogOperations;
        }

        public static Translog readTranslogFrom(StreamInput in) throws IOException {
            Translog t = new Translog();
            t.readFrom(in);
            return t;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            currentTranslogOperations = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(currentTranslogOperations);
        }
    }

    // XXX - (andrew) - Change timing to setters/getters for long values

    public static class Timer implements Streamable {
        private long start = 0;
        private long stop = 0;

        private Timer() { }

        public void start() {
            start = System.currentTimeMillis();
        }

        public long startTime() {
            return start;
        }

        public void stop() {
            stop = System.currentTimeMillis();
        }

        public long stopTime() {
            return stop;
        }

        public long elapsed() {
            if (start == 0) {
                return 0;
            } else if (stop == 0) {
                return System.currentTimeMillis() - start;
            } else {
                return stop - start;
            }
        }

        public static Timer readTimerFrom(StreamInput in) throws IOException {
            Timer timer = new Timer();
            timer.readFrom(in);
            return timer;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            start = in.readVLong();
            stop = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(start);
            out.writeVLong(stop);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("id", shardId.id());
        builder.field("type", type.toString().toLowerCase(Locale.ROOT));
        builder.field("state", state.toString().toLowerCase(Locale.ROOT));
        builder.field("complete", completed);
        builder.field("elapsed", timer.elapsed());

        builder.startObject("source");
        if (restoreSource != null) {
            restoreSource.toXContent(builder, params);
        } else if (sourceNode != null) {
            builder.field("id", sourceNode.getId());
            builder.field("hostname", sourceNode.getHostName());
            builder.field("address", sourceNode.getHostAddress());
        }
        builder.endObject();

        builder.startObject("target");
        if (targetNode != null) {
            builder.field("id", targetNode.getId());
            builder.field("hostname", targetNode.getHostName());
            builder.field("address", targetNode.getHostAddress());
        }
        builder.endObject();

        builder.startObject("index_metrics");
        builder.field("elapsed", indexTimer.elapsed());
        builder.field("version", index.version);
        builder.field("number_of_files", index.numberOfFiles);
        builder.field("number_of_files_recovered", index.numberOfFilesRecovered());
        builder.field("percent_of_files_recovered", safePercent(index.numberOfFilesRecovered(), index.numberOfFiles));
        builder.field("total_size_in_bytes", index.totalSize);
        builder.field("number_of_bytes_recovered", index.numberOfBytesRecovered());
        builder.field("percent_of_bytes_recovered", safePercent(index.numberOfBytesRecovered(), index.totalSize));
        builder.endObject();

        builder.startObject("translog_metrics");
        builder.field("elapsed", translogTimer.elapsed());
        builder.field("current_translog_operations", translog.currentTranslogOperations());
        builder.endObject();

        return builder;
    }

    private String safePercent(long a, long b) {
        if (b == 0) {
            return String.format(Locale.ROOT, "%1.1f%%", 0.0f);
        } else {
            return String.format(Locale.ROOT, "%1.1f%%", 100.0 * (float) a / b);
        }
    }
}
