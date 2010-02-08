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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.concurrent.NotThreadSafe;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.lease.Releasable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
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
    SizeValue estimateMemorySize();

    /**
     * Creates a new transaction log internally. Note, users of this class should make
     * sure that no operations are performed on the trans log when this is called.
     */
    void newTranslog();

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
     * Closes the transaction log.
     */
    void close();

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    @NotThreadSafe
    static interface Snapshot extends Iterable<Operation>, Releasable, Streamable {

        /**
         * The id of the translog the snapshot was taken with.
         */
        long translogId();

        /**
         * The number of translog operations in the snapshot.
         */
        int size();

        Iterable<Operation> skipTo(int skipTo);
    }

    /**
     * A generic interface representing an operation perfomed on the transaction log.
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

        void execute(IndexShard indexShard) throws ElasticSearchException;
    }

    static class Create implements Operation {
        private String id;
        private String type;
        private String source;

        public Create() {
        }

        public Create(Engine.Create create) {
            this(create.type(), create.id(), create.source());
        }

        public Create(String type, String id, String source) {
            this.id = id;
            this.type = type;
            this.source = source;
        }

        @Override public Type opType() {
            return Type.CREATE;
        }

        @Override public long estimateSize() {
            return ((id.length() + type.length() + source.length()) * 2) + 12;
        }

        public String id() {
            return this.id;
        }

        public String source() {
            return this.source;
        }

        public String type() {
            return this.type;
        }

        @Override public void execute(IndexShard indexShard) throws ElasticSearchException {
            indexShard.create(type, id, source);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            id = in.readUTF();
            type = in.readUTF();
            source = in.readUTF();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeUTF(source);
        }
    }

    static class Index implements Operation {
        private String id;
        private String type;
        private String source;

        public Index() {
        }

        public Index(Engine.Index index) {
            this(index.type(), index.id(), index.source());
        }

        public Index(String type, String id, String source) {
            this.type = type;
            this.id = id;
            this.source = source;
        }

        @Override public Type opType() {
            return Type.SAVE;
        }

        @Override public long estimateSize() {
            return ((id.length() + type.length() + source.length()) * 2) + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String source() {
            return this.source;
        }

        @Override public void execute(IndexShard indexShard) throws ElasticSearchException {
            indexShard.index(type, id, source);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            id = in.readUTF();
            type = in.readUTF();
            source = in.readUTF();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeUTF(source);
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

        @Override public void execute(IndexShard indexShard) throws ElasticSearchException {
            indexShard.delete(uid);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            uid = new Term(in.readUTF(), in.readUTF());
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(uid.field());
            out.writeUTF(uid.text());
        }
    }

    static class DeleteByQuery implements Operation {
        private String source;
        @Nullable private String queryParserName;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.queryParserName(), deleteByQuery.types());
        }

        public DeleteByQuery(String source, @Nullable String queryParserName, String... types) {
            this.queryParserName = queryParserName;
            this.source = source;
            this.types = types;
        }

        @Override public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override public long estimateSize() {
            return ((source.length() + (queryParserName == null ? 0 : queryParserName.length())) * 2) + 8;
        }

        public String queryParserName() {
            return this.queryParserName;
        }

        public String source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }

        @Override public void execute(IndexShard indexShard) throws ElasticSearchException {
            indexShard.deleteByQuery(source, queryParserName, types);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            source = in.readUTF();
            if (in.readBoolean()) {
                queryParserName = in.readUTF();
            }
            int typesSize = in.readInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readUTF();
                }
            }
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(source);
            if (queryParserName == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(queryParserName);
            }
            out.writeInt(types.length);
            for (String type : types) {
                out.writeUTF(type);
            }
        }
    }
}
