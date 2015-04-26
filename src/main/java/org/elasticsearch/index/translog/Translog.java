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

package org.elasticsearch.index.translog;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShardComponent;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;


/**
 *
 */
public interface Translog extends IndexShardComponent, Closeable, Accountable {

    static ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb");

    public static final String TRANSLOG_ID_KEY = "translog_id";

    void updateBuffer(ByteSizeValue bufferSize);

    /**
     * Returns the id of the current transaction log.
     */
    long currentId();

    /**
     * Returns the number of operations in the transaction log.
     */
    int estimatedNumberOfOperations();

    /**
     * Returns the size in bytes of the translog.
     */
    long translogSizeInBytes();

    /**
     * Creates a new transaction log internally.
     * <p/>
     * <p>Can only be called by one thread.
     * @param id the translog id for the new translog
     */
    void newTranslog(long id) throws TranslogException, IOException;

    /**
     * Creates a new transient translog, where added ops will be added to the current one, and to
     * it.
     * <p/>
     * <p>Can only be called by one thread.
     */
    void newTransientTranslog(long id) throws TranslogException;

    /**
     * Swaps the transient translog to be the current one.
     * <p/>
     * <p>Can only be called by one thread.
     */
    void makeTransientCurrent() throws IOException;

    /**
     * Reverts back to not have a transient translog.
     */
    void revertTransient() throws IOException;

    /**
     * Adds a create operation to the transaction log.
     */
    Location add(Operation operation) throws TranslogException;

    Translog.Operation read(Location location);

    /**
     * Snapshots the current transaction log allowing to safely iterate over the snapshot.
     */
    Snapshot snapshot() throws TranslogException;

    /**
     * Snapshots the delta between the current state of the translog, and the state defined
     * by the provided snapshot. If a new translog has been created after the provided snapshot
     * has been take, will return a snapshot on the current trasnlog.
     */
    Snapshot snapshot(Snapshot snapshot);

    /**
     * Clears unreferenced transaction logs.
     *
     * @return the number of clean up files
     */
    int clearUnreferenced();

    /**
     * Sync's the translog.
     */
    void sync() throws IOException;

    boolean syncNeeded();

    void syncOnEachOperation(boolean syncOnEachOperation);

    /**
     * Returns all translog locations as absolute paths.
     * These paths don't contain actual translog files they are
     * directories holding the transaction logs.
     */
    public Path location();

    /**
     * Returns the translog filename for the given id.
     */
    String getFilename(long translogId);

    /**
     * return stats
     */
    TranslogStats stats();

    /**
     * Returns the largest translog id present in all locations or <tt>-1</tt> if no translog is present.
     */
    long findLargestPresentTranslogId() throws IOException;

    /**
     * Returns an OperationIterator to iterate over all translog entries in the given translog ID.
     * @throws java.io.FileNotFoundException if the file for the translog ID can not be found
     */
    OperationIterator openIterator(long translogId) throws IOException;

    /**
     * Iterator for translog operations.
     */
    public static interface OperationIterator extends Releasable {
        /**
         * Returns the next operation in the translog or <code>null</code> if we reached the end of the stream.
         */
        public Translog.Operation next() throws IOException;
    }

    static class Location implements Accountable {

        public final long translogId;
        public final long translogLocation;
        public final int size;

        public Location(long translogId, long translogLocation, int size) {
            this.translogId = translogId;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2*RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "[id: " + translogId + ", location: " + translogLocation + ", size: " + size + "]";
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    static interface Snapshot extends OperationIterator {

        /**
         * The id of the translog the snapshot was taken with.
         */
        long translogId();

        /**
         * Returns the current position in the translog stream
         */
        long position();

        /**
         * Returns the internal length (*not* number of operations) of this snapshot.
         */
        long length();

        /**
         * The total number of operations in the translog.
         */
        int estimatedTotalOperations();

        /**
         * Seek to the specified position in the translog stream
         */
        void seekTo(long position);

        /**
         * The length in bytes of this stream.
         */
        long lengthInBytes();
    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    static interface Operation extends Streamable {
        static enum Type {
            CREATE((byte) 1),
            SAVE((byte) 2),
            DELETE((byte) 3),
            DELETE_BY_QUERY((byte) 4);

            private final byte id;

            private Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch (id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return SAVE;
                    case 3:
                        return DELETE;
                    case 4:
                        return DELETE_BY_QUERY;
                    default:
                        throw new ElasticsearchIllegalArgumentException("No type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();
    }

    static class Source {
        public final BytesReference source;
        public final String routing;
        public final String parent;
        public final long timestamp;
        public final long ttl;

        public Source(BytesReference source, String routing, String parent, long timestamp, long ttl) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    static class Create implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Create() {
        }

        public Create(Engine.Create create) {
            this.id = create.id();
            this.type = create.type();
            this.source = create.source();
            this.routing = create.routing();
            this.parent = create.parent();
            this.timestamp = create.timestamp();
            this.ttl = create.ttl();
            this.version = create.version();
            this.versionType = create.versionType();
        }

        public Create(String type, String id, byte[] source) {
            this.id = id;
            this.type = type;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.CREATE;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String id() {
            return this.id;
        }

        public BytesReference source() {
            return this.source;
        }

        public String type() {
            return this.type;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readString();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readString();
                }
            }
            if (version >= 3) {
                this.version = in.readLong();
            }
            if (version >= 4) {
                this.timestamp = in.readLong();
            }
            if (version >= 5) {
                this.ttl = in.readLong();
            }
            if (version >= 6) {
                this.versionType = VersionType.fromValue(in.readByte());
            }

            assert versionType.validateVersionForWrites(version);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
        }
    }

    static class Index implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;

        public Index() {
        }

        public Index(Engine.Index index) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.parent = index.parent();
            this.version = index.version();
            this.timestamp = index.timestamp();
            this.ttl = index.ttl();
            this.versionType = index.versionType();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.SAVE;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public BytesReference source() {
            return this.source;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            try {
                if (version >= 1) {
                    if (in.readBoolean()) {
                        routing = in.readString();
                    }
                }
                if (version >= 2) {
                    if (in.readBoolean()) {
                        parent = in.readString();
                    }
                }
                if (version >= 3) {
                    this.version = in.readLong();
                }
                if (version >= 4) {
                    this.timestamp = in.readLong();
                }
                if (version >= 5) {
                    this.ttl = in.readLong();
                }
                if (version >= 6) {
                    this.versionType = VersionType.fromValue(in.readByte());
                }
            } catch (Exception e) {
                throw new ElasticsearchException("failed to read [" + type + "][" + id + "]", e);
            }

            assert versionType.validateVersionForWrites(version);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
        }
    }

    static class Delete implements Operation {
        public static final int SERIALIZATION_FORMAT = 2;

        private Term uid;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Delete() {
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid());
            this.version = delete.version();
            this.versionType = delete.versionType();
        }

        public Delete(Term uid) {
            this.uid = uid;
        }

        public Delete(Term uid, long version, VersionType versionType) {
            this.uid = uid;
            this.version = version;
            this.versionType = versionType;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        @Override
        public Source getSource(){
            throw new ElasticsearchIllegalStateException("trying to read doc source from delete operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            uid = new Term(in.readString(), in.readString());
            if (version >= 1) {
                this.version = in.readLong();
            }
            if (version >= 2) {
                this.versionType = VersionType.fromValue(in.readByte());
            }
            assert versionType.validateVersionForWrites(version);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(uid.field());
            out.writeString(uid.text());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
        }
    }

    static class DeleteByQuery implements Operation {

        public static final int SERIALIZATION_FORMAT = 2;
        private BytesReference source;
        @Nullable
        private String[] filteringAliases;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.types());
        }

        public DeleteByQuery(BytesReference source, String[] filteringAliases, String... types) {
            this.source = source;
            this.types = types == null ? Strings.EMPTY_ARRAY : types;
            this.filteringAliases = filteringAliases;
        }

        @Override
        public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override
        public long estimateSize() {
            return source.length() + 8;
        }

        public BytesReference source() {
            return this.source;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public String[] types() {
            return this.types;
        }

        @Override
        public Source getSource() {
            throw new ElasticsearchIllegalStateException("trying to read doc source from delete_by_query operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            source = in.readBytesReference();
            if (version < 2) {
                // for query_parser_name, which was removed
                if (in.readBoolean()) {
                    in.readString();
                }
            }
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readString();
                }
            }
            if (version >= 1) {
                int aliasesSize = in.readVInt();
                if (aliasesSize > 0) {
                    filteringAliases = new String[aliasesSize];
                    for (int i = 0; i < aliasesSize; i++) {
                        filteringAliases[i] = in.readString();
                    }
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeBytesReference(source);
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeString(type);
            }
            if (filteringAliases != null) {
                out.writeVInt(filteringAliases.length);
                for (String alias : filteringAliases) {
                    out.writeString(alias);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }
}
