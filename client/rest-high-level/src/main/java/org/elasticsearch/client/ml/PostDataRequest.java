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
package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Request to post data to a Machine Learning job
 */
public class PostDataRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField RESET_START = new ParseField("reset_start");
    public static final ParseField RESET_END = new ParseField("reset_end");
    public static final ParseField CONTENT_TYPE = new ParseField("content_type");

    public static final ConstructingObjectParser<PostDataRequest, Void> PARSER =
        new ConstructingObjectParser<>("post_data_request",
            (a) -> new PostDataRequest((String)a[0], XContentType.fromMediaTypeOrFormat((String)a[1]), new byte[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CONTENT_TYPE);
        PARSER.declareStringOrNull(PostDataRequest::setResetEnd, RESET_END);
        PARSER.declareStringOrNull(PostDataRequest::setResetStart, RESET_START);
    }

    private final String jobId;
    private final XContentType xContentType;
    private final BytesReference content;
    private String resetStart;
    private String resetEnd;

    /**
     * Create a new PostDataRequest object
     *
     * @param jobId non-null jobId of the job to post data to
     * @param xContentType content type of the data to post. Only {@link XContentType#JSON} or {@link XContentType#SMILE} are supported
     * @param content bulk serialized content in the format of the passed {@link XContentType}
     */
    public PostDataRequest(String jobId, XContentType xContentType, BytesReference content) {
        this.jobId = Objects.requireNonNull(jobId, "job_id must not be null");
        this.xContentType = Objects.requireNonNull(xContentType, "content_type must not be null");
        this.content = Objects.requireNonNull(content, "content must not be null");
    }

    /**
     * Create a new PostDataRequest object referencing the passed {@code byte[]} content
     *
     * @param jobId non-null jobId of the job to post data to
     * @param xContentType content type of the data to post. Only {@link XContentType#JSON} or {@link XContentType#SMILE} are supported
     * @param content bulk serialized content in the format of the passed {@link XContentType}
     */
    public PostDataRequest(String jobId, XContentType xContentType, byte[] content) {
        this(jobId, xContentType, new BytesArray(content));
    }

    /**
     * Create a new PostDataRequest object referencing the passed {@link JsonBuilder} object
     *
     * @param jobId non-null jobId of the job to post data to
     * @param builder {@link JsonBuilder} object containing documents to be serialized and sent in {@link XContentType#JSON} format
     */
    public PostDataRequest(String jobId, JsonBuilder builder) {
        this(jobId, XContentType.JSON, builder.build());
    }

    public String getJobId() {
        return jobId;
    }

    public String getResetStart() {
        return resetStart;
    }

    /**
     * Specifies the start of the bucket resetting range
     *
     * @param resetStart String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO 8601 string
     */
    public void setResetStart(String resetStart) {
        this.resetStart = resetStart;
    }

    public String getResetEnd() {
        return resetEnd;
    }

    /**
     * Specifies the end of the bucket resetting range
     *
     * @param resetEnd String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO 8601 string
     */
    public void setResetEnd(String resetEnd) {
        this.resetEnd = resetEnd;
    }

    public BytesReference getContent() {
        return content;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public int hashCode() {
        //We leave out the content for server side parity
        return Objects.hash(jobId, resetStart, resetEnd, xContentType);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        //We leave out the content for server side parity
        PostDataRequest other = (PostDataRequest) obj;
        return Objects.equals(jobId, other.jobId) &&
            Objects.equals(resetStart, other.resetStart) &&
            Objects.equals(resetEnd, other.resetEnd) &&
            Objects.equals(xContentType, other.xContentType);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(CONTENT_TYPE.getPreferredName(), xContentType.mediaType());
        if (resetEnd != null) {
            builder.field(RESET_END.getPreferredName(), resetEnd);
        }
        if (resetStart != null) {
            builder.field(RESET_START.getPreferredName(), resetStart);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Class for incrementally building a bulk document request in {@link XContentType#JSON} format
     */
    public static class JsonBuilder {

        private final List<ByteBuffer> bytes = new ArrayList<>();

        /**
         * Add a document via a {@code byte[]} array
         *
         * @param doc {@code byte[]} array of a serialized JSON object
         */
        public JsonBuilder addDoc(byte[] doc) {
            bytes.add(ByteBuffer.wrap(doc));
            return this;
        }

        /**
         * Add a document via a serialized JSON String
         *
         * @param doc a serialized JSON String
         */
        public JsonBuilder addDoc(String doc) {
            bytes.add(ByteBuffer.wrap(doc.getBytes(StandardCharsets.UTF_8)));
            return this;
        }

        /**
         * Add a document via an object map
         *
         * @param doc document object to add to bulk request
         * @throws IOException on parsing/serialization errors
         */
        public JsonBuilder addDoc(Map<String, Object> doc) throws IOException {
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.map(doc);
                bytes.add(ByteBuffer.wrap(BytesReference.toBytes(BytesReference.bytes(builder))));
            }
            return this;
        }

        private BytesReference build() {
            ByteBuffer[] buffers = bytes.toArray(new ByteBuffer[bytes.size()]);
            return BytesReference.fromByteBuffers(buffers);
        }

    }
}
