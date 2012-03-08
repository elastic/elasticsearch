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

package org.elasticsearch.action.explain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.ExplainSourceBuilder;

/**
 * A request to execute an explain of a particular document against a query. Best created using
 * {@link org.elasticsearch.client.Requests#explainRequest(String)}.
 * <p/>
 * <p>
 * Note, the explain {@link #source(org.elasticsearch.search.builder.ExplainSourceBuilder)} is required. The explain
 * source is the different explain options.
 * <p/>
 * <p>
 * There is an option to specify an addition explain source using the
 * {@link #extraSource(org.elasticsearch.search.builder.ExplainSourceBuilder)}.
 * 
 * @see org.elasticsearch.client.Requests#explainRequest(String)
 * @see org.elasticsearch.client.Client#explain(ExplainRequest)
 * @see ExplainResponse
 */
public class ExplainRequest extends SingleShardOperationRequest {

    protected String type;
    protected String id;
    protected String routing;
    protected String preference;

    private boolean refresh = false;

    private byte[] source;
    private int sourceOffset;
    private int sourceLength;
    private boolean sourceUnsafe;

    private byte[] extraSource;
    private int extraSourceOffset;
    private int extraSourceLength;
    private boolean extraSourceUnsafe;

    ExplainRequest() {
        type = "_all";
    }

    /**
     * Constructs a new explain request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public ExplainRequest(String index) {
        super(index);
        this.type = "_all";
    }

    /**
     * Constructs a new explain request against the specified index with the type and id.
     * 
     * @param index
     *            The index to get the document from
     * @param type
     *            The type of the document
     * @param id
     *            The id of the document
     */
    public ExplainRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    public void beforeStart() {
        // we always copy over if needed, the reason is that a request might fail while being search remotely
        // and then we need to keep the buffer around
        if (source != null && sourceUnsafe) {
            source = Arrays.copyOfRange(source, sourceOffset, sourceOffset + sourceLength);
            sourceOffset = 0;
            sourceUnsafe = false;
        }
        if (extraSource != null && extraSourceUnsafe) {
            extraSource = Arrays.copyOfRange(extraSource, extraSourceOffset, extraSourceOffset + extraSourceLength);
            extraSourceOffset = 0;
            extraSourceUnsafe = false;
        }
    }

    /**
     * Sets the index of the document to explain.
     */
    @Required
    public ExplainRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * Sets the type of the document to explain.
     */
    public ExplainRequest type(@Nullable String type) {
        if (type == null) {
            type = "_all";
        }
        this.type = type;
        return this;
    }

    /**
     * Sets the id of the document to explain.
     */
    @Required
    public ExplainRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard and not the id.
     */
    public ExplainRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the explain. Defaults to randomize across shards. Can be set to <tt>_local</tt> to
     * prefer local shards, <tt>_primary</tt> to execute only on primary shards, or a custom value, which guarantees
     * that the same order will be used across different requests.
     */
    public ExplainRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * The source of the query of the explain request.
     */
    public ExplainRequest source(ExplainSourceBuilder sourceBuilder) {
        BytesStream bos = sourceBuilder.buildAsBytesStream(Requests.CONTENT_TYPE);
        this.source = bos.underlyingBytes();
        this.sourceOffset = 0;
        this.sourceLength = bos.size();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the query of the explain request. Consider using either {@link #source(byte[])} or
     * {@link #source(org.elasticsearch.search.builder.ExplainSourceBuilder)}.
     */
    public ExplainRequest source(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.source = result.result;
        this.sourceOffset = 0;
        this.sourceLength = result.length;
        this.sourceUnsafe = true;
        return this;
    }

    /**
     * The source of the query of the explain request in the form of a map.
     */
    public ExplainRequest source(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public ExplainRequest source(XContentBuilder builder) {
        try {
            this.source = builder.underlyingBytes();
            this.sourceOffset = 0;
            this.sourceLength = builder.underlyingBytesLength();
            this.sourceUnsafe = false;
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + builder + "]", e);
        }
    }

    /**
     * The source of the query to explain.
     */
    public ExplainRequest source(byte[] source) {
        return source(source, 0, source.length, false);
    }

    /**
     * The source of the query to explain.
     */
    public ExplainRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    /**
     * The source of the query to explain.
     */
    public ExplainRequest source(byte[] source, int offset, int length, boolean unsafe) {
        this.source = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * The source of the query to explain.
     */
    public byte[] source() {
        return source;
    }

    public int sourceOffset() {
        return sourceOffset;
    }

    public int sourceLength() {
        return sourceLength;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public ExplainRequest extraSource(ExplainSourceBuilder sourceBuilder) {
        if (sourceBuilder == null) {
            extraSource = null;
            return this;
        }
        BytesStream bos = sourceBuilder.buildAsBytesStream(Requests.CONTENT_TYPE);
        this.extraSource = bos.underlyingBytes();
        this.extraSourceOffset = 0;
        this.extraSourceLength = bos.size();
        this.extraSourceUnsafe = false;
        return this;
    }

    public ExplainRequest extraSource(Map extraSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(extraSource);
            return extraSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public ExplainRequest extraSource(XContentBuilder builder) {
        try {
            this.extraSource = builder.underlyingBytes();
            this.extraSourceOffset = 0;
            this.extraSourceLength = builder.underlyingBytesLength();
            this.extraSourceUnsafe = false;
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + builder + "]", e);
        }
    }

    /**
     * Allows to provide additional source that will use used as well.
     */
    public ExplainRequest extraSource(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.extraSource = result.result;
        this.extraSourceOffset = 0;
        this.extraSourceLength = result.length;
        this.extraSourceUnsafe = true;
        return this;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public ExplainRequest extraSource(byte[] source) {
        return extraSource(source, 0, source.length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public ExplainRequest extraSource(byte[] source, int offset, int length) {
        return extraSource(source, offset, length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public ExplainRequest extraSource(byte[] source, int offset, int length, boolean unsafe) {
        this.extraSource = source;
        this.extraSourceOffset = offset;
        this.extraSourceLength = length;
        this.extraSourceUnsafe = unsafe;
        return this;
    }

    /**
     * Additional source of the query to explain.
     */
    public byte[] extraSource() {
        return this.extraSource;
    }

    public int extraSourceOffset() {
        return extraSourceOffset;
    }

    public int extraSourceLength() {
        return extraSourceLength;
    }

    /**
     * Should a refresh be executed before this explain operation causing the operation to return the latest value.
     * Note, heavy explain should not set this to <tt>true</tt>. Defaults to <tt>false</tt>.
     */
    public ExplainRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public ExplainRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    @Override
    public ExplainRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        type = in.readUTF();
        id = in.readUTF();
        if (in.readBoolean()) {
            routing = in.readUTF();
        }
        if (in.readBoolean()) {
            preference = in.readUTF();
        }

        BytesHolder bytes = in.readBytesReference();
        sourceUnsafe = false;
        source = bytes.bytes();
        sourceOffset = bytes.offset();
        sourceLength = bytes.length();

        bytes = in.readBytesReference();
        extraSourceUnsafe = false;
        extraSource = bytes.bytes();
        extraSourceOffset = bytes.offset();
        extraSourceLength = bytes.length();

        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeUTF(type);
        out.writeUTF(id);
        if (routing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(routing);
        }
        if (preference == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(preference);
        }

        out.writeBytesHolder(source, sourceOffset, sourceLength);
        out.writeBytesHolder(extraSource, extraSourceOffset, extraSourceLength);

        out.writeBoolean(refresh);
    }

    @Override
    public String toString() {
        return "[" + index + "][" + type + "][" + id + "]: routing [" + routing + "]";
    }

}
