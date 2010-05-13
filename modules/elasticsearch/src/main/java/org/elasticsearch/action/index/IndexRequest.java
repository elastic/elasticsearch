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

package org.elasticsearch.action.index;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.action.Actions.*;

/**
 * Index request to index a typed JSON document into a specific index and make it searchable. Best
 * created using {@link org.elasticsearch.client.Requests#indexRequest(String)}.
 *
 * <p>The index requires the {@link #index()}, {@link #type(String)}, {@link #id(String)} and
 * {@link #source(byte[])} to be set.
 *
 * <p>The source (content to index) can be set in its bytes form using ({@link #source(byte[])}),
 * its string form ({@link #source(String)}) or using a {@link org.elasticsearch.util.xcontent.builder.XContentBuilder}
 * ({@link #source(org.elasticsearch.util.xcontent.builder.XContentBuilder)}).
 *
 * <p>If the {@link #id(String)} is not set, it will be automatically generated.
 *
 * @author kimchy (shay.banon)
 * @see IndexResponse
 * @see org.elasticsearch.client.Requests#indexRequest(String)
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexRequest extends ShardReplicationOperationRequest {

    /**
     * Operation type controls if the type of the index operation.
     */
    public static enum OpType {
        /**
         * Index the source. If there an existing document with the id, it will
         * be replaced.
         */
        INDEX((byte) 0),
        /**
         * Creates the resource. Simply adds it to the index, if there is an existing
         * document with the id, then it won't be removed.
         */
        CREATE((byte) 1);

        private byte id;

        OpType(byte id) {
            this.id = id;
        }

        /**
         * The internal representation of the operation type.
         */
        public byte id() {
            return id;
        }

        /**
         * Constructs the operation type from its internal representation.
         */
        public static OpType fromId(byte id) {
            if (id == 0) {
                return INDEX;
            } else if (id == 1) {
                return CREATE;
            } else {
                throw new ElasticSearchIllegalArgumentException("No type match for [" + id + "]");
            }
        }
    }

    private String type;
    private String id;

    private byte[] source;
    private int sourceOffset;
    private int sourceLength;
    private boolean sourceUnsafe;

    private OpType opType = OpType.INDEX;

    public IndexRequest() {
    }

    /**
     * Constructs a new index request against the specific index. The {@link #type(String)},
     * {@link #id(String)} and {@link #source(byte[])} must be set.
     */
    public IndexRequest(String index) {
        this.index = index;
    }

    /**
     * Constructs a new index request against the index, type, id and using the source.
     *
     * @param index  The index to index into
     * @param type   The type to index into
     * @param id     The id of document
     * @param source The JSON source document
     */
    public IndexRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override protected void beforeLocalFork() {
        source();
    }

    /**
     * Sets the index the index operation will happen on.
     */
    @Override public IndexRequest index(String index) {
        super.index(index);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public IndexRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    @Override public IndexRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    /**
     * The type of the indexed document.
     */
    String type() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    @Required public IndexRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document. If not set, will be automatically generated.
     */
    String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document. If not set, will be automatically generated.
     */
    public IndexRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * The source of the JSON document to index.
     */
    byte[] source() {
        if (sourceUnsafe) {
            source = Arrays.copyOfRange(source, sourceOffset, sourceLength);
            sourceOffset = 0;
            sourceUnsafe = false;
        }
        return source;
    }

    /**
     * Writes the Map as a JSON.
     *
     * @param source The map to index
     */
    @Required public IndexRequest source(Map source) throws ElasticSearchGenerationException {
        return source(source, XContentType.JSON);
    }

    /**
     * Writes the Map as the provided content type.
     *
     * @param source The map to index
     */
    @Required public IndexRequest source(Map source, XContentType contentType) throws ElasticSearchGenerationException {
        try {
            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     *
     * <p>Note, its preferable to either set it using {@link #source(org.elasticsearch.util.xcontent.builder.XContentBuilder)}
     * or using the {@link #source(byte[])}.
     */
    @Required public IndexRequest source(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.source = result.result;
        this.sourceOffset = 0;
        this.sourceLength = result.length;
        this.sourceUnsafe = true;
        return this;
    }

    /**
     * Sets the content source to index.
     */
    @Required public IndexRequest source(XContentBuilder sourceBuilder) {
        try {
            source = sourceBuilder.unsafeBytes();
            sourceOffset = 0;
            sourceLength = sourceBuilder.unsafeBytesLength();
            sourceUnsafe = true;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + sourceBuilder + "]", e);
        }
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @return
     */
    @Required public IndexRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    /**
     * Sets the document to index in bytes form.
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @param unsafe Is the byte array safe to be used form a different thread
     * @return
     */
    @Required public IndexRequest source(byte[] source, int offset, int length, boolean unsafe) {
        this.source = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public IndexRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequest opType(OpType opType) {
        this.opType = opType;
        return this;
    }

    /**
     * Sets a string representation of the {@link #opType(org.elasticsearch.action.index.IndexRequest.OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequest opType(String opType) throws ElasticSearchIllegalArgumentException {
        if ("create".equals(opType)) {
            return opType(OpType.CREATE);
        } else if ("index".equals(opType)) {
            return opType(OpType.INDEX);
        } else {
            throw new ElasticSearchIllegalArgumentException("No index opType matching [" + opType + "]");
        }
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link OpType#CREATE}.
     */
    public IndexRequest create(boolean create) {
        if (create) {
            return opType(OpType.CREATE);
        } else {
            return opType(OpType.INDEX);
        }
    }

    /**
     * The type of operation to perform.
     */
    public OpType opType() {
        return this.opType;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readUTF();
        if (in.readBoolean()) {
            id = in.readUTF();
        }

        sourceUnsafe = false;
        sourceOffset = 0;
        sourceLength = in.readVInt();
        source = new byte[sourceLength];
        in.readFully(source);

        opType = OpType.fromId(in.readByte());
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(type);
        if (id == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(id);
        }
        out.writeVInt(sourceLength);
        out.writeBytes(source, sourceOffset, sourceLength);
        out.writeByte(opType.id());
    }

    @Override public String toString() {
        return "[" + index + "][" + type + "][" + id + "], source[" + Unicode.fromBytes(source, sourceOffset, sourceLength) + "]";
    }
}