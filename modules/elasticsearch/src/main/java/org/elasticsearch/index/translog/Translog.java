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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.NotThreadSafe;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShardComponent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public interface Translog extends IndexShardComponent {

    /**
     * Returns the id of the current transaction log.
     */
    long currentId();

    /**
     * Returns the number of operations in the transaction log.
     */
    int size();

    /**
     * The estimated memory size this translog is taking.
     */
    ByteSizeValue estimateMemorySize();

    /**
     * Creates a new transaction log internally. Note, users of this class should make
     * sure that no operations are performed on the trans log when this is called.
     */
    void newTranslog() throws TranslogException;

    /**
     * Creates a new transaction log internally. Note, users of this class should make
     * sure that no operations are performed on the trans log when this is called.
     */
    void newTranslog(long id) throws TranslogException;

    /**
     * Adds a create operation to the transaction log.
     */
    void add(Operation operation) throws TranslogException;

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
     */
    void close(boolean delete);

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
        int totalOperations();

        /**
         * The number of operations in this snapshot.
         */
        int snapshotOperations();

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
    }

    static class Create implements Operation {
        private String id;
        private String type;
        private byte[] source;
        private String routing;
        private String parent;

        public Create() {
        }

        public Create(Engine.Create create) {
            this(create.type(), create.id(), create.source());
            this.routing = create.routing();
            this.parent = create.parent();
        }

        public Create(String type, String id, byte[] source) {
            this.id = id;
            this.type = type;
            this.source = source;
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

        public String type() {
            return this.type;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();
            source = new byte[in.readVInt()];
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
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(2); // version
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeVInt(source.length);
            out.writeBytes(source);
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
        }
    }

    static class Index implements Operation {
        private String id;
        private String type;
        private byte[] source;
        private String routing;
        private String parent;

        public Index() {
        }

        public Index(Engine.Index index) {
            this(index.type(), index.id(), index.source());
            this.routing = index.routing();
            this.parent = index.parent();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = source;
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

        public byte[] source() {
            return this.source;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readUTF();
            type = in.readUTF();
            source = new byte[in.readVInt()];
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
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(2); // version
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeVInt(source.length);
            out.writeBytes(source);
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
        }
    }

    static class Delete implements Operation {
        private Term uid;

        public Delete() {
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid());
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

        @Override public void readFrom(StreamInput in) throws IOException {
            in.readVInt(); // version
            uid = new Term(in.readUTF(), in.readUTF());
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(0); // version
            out.writeUTF(uid.field());
            out.writeUTF(uid.text());
        }
    }

    static class DeleteByQuery implements Operation {
        private byte[] source;
        @Nullable private String queryParserName;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.queryParserName(), deleteByQuery.types());
        }

        public DeleteByQuery(byte[] source, @Nullable String queryParserName, String... types) {
            this.queryParserName = queryParserName;
            this.source = source;
            this.types = types;
        }

        @Override public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override public long estimateSize() {
            return source.length + ((queryParserName == null ? 0 : queryParserName.length()) * 2) + 8;
        }

        public String queryParserName() {
            return this.queryParserName;
        }

        public byte[] source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            in.readVInt(); // version
            source = new byte[in.readVInt()];
            in.readFully(source);
            if (in.readBoolean()) {
                queryParserName = in.readUTF();
            }
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readUTF();
                }
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(0); // version
            out.writeVInt(source.length);
            out.writeBytes(source);
            if (queryParserName == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(queryParserName);
            }
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeUTF(type);
            }
        }
    }
}
