/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.NotThreadSafe;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShardComponent;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public interface Translog extends IndexShardComponent {

    public static final String TRANSLOG_ID_KEY = "translog_id";

    /**
     * Returns the id of the current transaction log.
     */
    long currentId();

    /**
     * Returns the number of operations in the transaction log.
     */
    int estimatedNumberOfOperations();

    /**
     * The estimated memory size this translog is taking.
     */
    long memorySizeInBytes();

    /**
     * Returns the size in bytes of the translog.
     */
    long translogSizeInBytes();

    /**
     * Creates a new transaction log internally.
     *
     * <p>Can only be called by one thread.
     */
    void newTranslog(long id) throws TranslogException;

    /**
     * Creates a new transient translog, where added ops will be added to the current one, and to
     * it.
     *
     * <p>Can only be called by one thread.
     */
    void newTransientTranslog(long id) throws TranslogException;

    /**
     * Swaps the transient translog to be the current one.
     *
     * <p>Can only be called by one thread.
     */
    void makeTransientCurrent();

    /**
     * Reverts back to not have a transient translog.
     */
    void revertTransient();

    /**
     * Adds a create operation to the transaction log.
     */
    Location add(Operation operation) throws TranslogException;

    byte[] read(Location location);

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
     * Clears unreferenced transaclogs.
     */
    void clearUnreferenced();

    /**
     * Sync's the translog.
     */
    void sync();

    void syncOnEachOperation(boolean syncOnEachOperation);

    /**
     * Closes the transaction log.
     *
     * <p>Can only be called by one thread.
     */
    void close(boolean delete);

    static class Location {
        public final long translogId;
        public final long translogLocation;
        public final int size;

        public Location(long translogId, long translogLocation, int size) {
            this.translogId = translogId;
            this.translogLocation = translogLocation;
            this.size = size;
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    @NotThreadSafe
    static interface Snapshot extends Releasable {

        /**
         * The id of the translog the snapshot was taken with.
         */
        long translogId();

        long position();

        /**
         * Returns the internal length (*not* number of operations) of this snapshot.
         */
        long length();

        /**
         * The total number of operations in the translog.
         */
        int estimatedTotalOperations();

        boolean hasNext();

        Operation next();

        void seekForward(long length);

        /**
         * Returns a stream of this snapshot.
         */
        InputStream stream() throws IOException;

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
                        throw new IllegalArgumentException("No type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source readSource(BytesStreamInput in) throws IOException;
    }

    static class Source {
        public final BytesHolder source;
        public final String routing;
        public final String parent;
        public final long timestamp;
        public final long ttl;

        public Source(BytesHolder source, String routing, String parent, long timestamp, long ttl) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    static class Create implements Operation {
        private String id;
        private String type;
        private byte[] source;
        private int sourceOffset;
        private int sourceLength;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;
        private long version;

        public Create() {
        }

        public Create(Engine.Create create) {
            this.id = create.id();
            this.type = create.type();
            this.source = create.source();
            this.sourceOffset = create.sourceOffset();
            this.sourceLength = create.sourceLength();
            this.routing = create.routing();
            this.parent = create.parent();
            this.timestamp = create.timestamp();
            this.ttl = create.ttl();
            this.version = create.version();
        }

        public Create(String type, String id, byte[] source) {
            this.id = id;
            this.type = type;
            this.source = source;
            this.sourceOffset = 0;
            this.sourceLength = source.length;
        }

        @Override public Type opType() {
            return Type.CREATE;
        }

        @Override public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length + 12;
        }

        public String id() {
            return this.id;
        }

        public byte[] source() {
            return this.source;
        }

        public int sourceOffset() {
            return this.sourceOffset;
        }

        public int sourceLength() {
            return this.sourceLength;
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

        @Override public Source readSource(BytesStreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();

            int length = in.readVInt();
            int offset = in.position();
            BytesHolder source = new BytesHolder(in.underlyingBuffer(), offset, length);
            in.skip(length);
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readUTF();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readUTF();
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
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();
            sourceOffset = 0;
            sourceLength = in.readVInt();
            source = new byte[sourceLength];
            in.readFully(source);
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readUTF();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readUTF();
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
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(5); // version
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeVInt(sourceLength);
            out.writeBytes(source, sourceOffset, sourceLength);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
        }
    }

    static class Index implements Operation {
        private String id;
        private String type;
        private long version;
        private byte[] source;
        private int sourceOffset;
        private int sourceLength;
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
            this.sourceOffset = index.sourceOffset();
            this.sourceLength = index.sourceLength();
            this.routing = index.routing();
            this.parent = index.parent();
            this.version = index.version();
            this.timestamp = index.timestamp();
            this.ttl = index.ttl();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = source;
            this.sourceOffset = 0;
            this.sourceLength = source.length;
        }

        @Override public Type opType() {
            return Type.SAVE;
        }

        @Override public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length + 12;
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

        public byte[] source() {
            return this.source;
        }

        public int sourceOffset() {
            return this.sourceOffset;
        }

        public int sourceLength() {
            return this.sourceLength;
        }

        public long version() {
            return this.version;
        }

        @Override public Source readSource(BytesStreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();

            int length = in.readVInt();
            int offset = in.position();
            BytesHolder source = new BytesHolder(in.underlyingBuffer(), offset, length);
            in.skip(length);
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readUTF();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readUTF();
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
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();
            sourceOffset = 0;
            sourceLength = in.readVInt();
            source = new byte[sourceLength];
            in.readFully(source);
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readUTF();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readUTF();
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
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(5); // version
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeVInt(sourceLength);
            out.writeBytes(source, sourceOffset, sourceLength);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
        }
    }

    static class Delete implements Operation {
        private Term uid;
        private long version;

        public Delete() {
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid());
            this.version = delete.version();
        }

        public Delete(Term uid) {
            this.uid = uid;
        }

        @Override public Type opType() {
            return Type.DELETE;
        }

        @Override public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        @Override public Source readSource(BytesStreamInput in) throws IOException {
            throw new ElasticSearchIllegalStateException("trying to read doc source from delete operation");
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            uid = new Term(in.readUTF(), in.readUTF());
            if (version >= 1) {
                this.version = in.readLong();
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(1); // version
            out.writeUTF(uid.field());
            out.writeUTF(uid.text());
            out.writeLong(version);
        }
    }

    static class DeleteByQuery implements Operation {
        private byte[] source;
        @Nullable private String[] filteringAliases;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.types());
        }

        public DeleteByQuery(byte[] source, String[] filteringAliases, String... types) {
            this.source = source;
            this.types = types == null ? Strings.EMPTY_ARRAY : types;
            this.filteringAliases = filteringAliases;
        }

        @Override public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override public long estimateSize() {
            return source.length + 8;
        }

        public byte[] source() {
            return this.source;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public String[] types() {
            return this.types;
        }

        @Override public Source readSource(BytesStreamInput in) throws IOException {
            throw new ElasticSearchIllegalStateException("trying to read doc source from delete_by_query operation");
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            source = new byte[in.readVInt()];
            in.readFully(source);
            if (version < 2) {
                // for query_parser_name, which was removed
                if (in.readBoolean()) {
                    in.readUTF();
                }
            }
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readUTF();
                }
            }
            if (version >= 1) {
                int aliasesSize = in.readVInt();
                if (aliasesSize > 0) {
                    filteringAliases = new String[aliasesSize];
                    for (int i = 0; i < aliasesSize; i++) {
                        filteringAliases[i] = in.readUTF();
                    }
                }
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(2); // version
            out.writeVInt(source.length);
            out.writeBytes(source);
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeUTF(type);
            }
            if (filteringAliases != null) {
                out.writeVInt(filteringAliases.length);
                for (String alias : filteringAliases) {
                    out.writeUTF(alias);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }
}
