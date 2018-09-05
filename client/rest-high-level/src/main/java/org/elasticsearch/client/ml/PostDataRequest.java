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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * POJO for posting data to a Machine Learning job
 */
public class PostDataRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField RESET_START = new ParseField("reset_start");
    public static final ParseField RESET_END = new ParseField("reset_end");
    public static final ParseField CONTENT_TYPE = new ParseField("content_type");

    public static final ConstructingObjectParser<PostDataRequest, Void> PARSER =
        new ConstructingObjectParser<>("post_data_request",
            (a) -> new PostDataRequest((String)a[0], XContentType.fromMediaTypeOrFormat((String)a[1])));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CONTENT_TYPE);
        PARSER.declareString(PostDataRequest::setResetEnd, RESET_END);
        PARSER.declareString(PostDataRequest::setResetStart, RESET_START);
    }

    private final String jobId;
    private final XContentType xContentType;
    private final List<BytesReference> bytesReferences;
    private final List<Map<String, Object>> objectMaps;
    private String resetStart;
    private String resetEnd;
    private BytesReference content;

    /**
     * PostDataRequest for sending data in JSON format
     * @param jobId non-null jobId of the job to post data to
     */
    public static PostDataRequest postJsonDataRequest(String jobId) {
        return new PostDataRequest(jobId, XContentType.JSON);
    }

    /**
     * PostDataRequest for sending data in SMILE format
     * @param jobId non-null jobId of the job to post data to
     */
    public static PostDataRequest postSmileDataRequest(String jobId) {
        return new PostDataRequest(jobId, XContentType.SMILE);
    }

    /**
     * Create a new PostDataRequest object
     *
     * @param jobId non-null jobId of the job to post data to
     * @param xContentType content type of the data to post. Only {@link XContentType#JSON} {@link XContentType#SMILE} are supported
     */
    public PostDataRequest(String jobId, XContentType xContentType) {
        this.jobId = Objects.requireNonNull(jobId, "job_id must not be null");
        this.xContentType = Objects.requireNonNull(xContentType, "content_type must not be null");
        this.bytesReferences = new ArrayList<>();
        this.objectMaps = new ArrayList<>();
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
     * @param resetStart String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
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
     * @param resetEnd String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setResetEnd(String resetEnd) {
        this.resetEnd = resetEnd;
    }

    /**
     * Gets the transformed content to post.
     *
     * This combines both documents added through {@link PostDataRequest#addDoc(BytesReference)} and {@link PostDataRequest#addDoc(Map)}
     * into a single BytesReference object according to the set XContentType for bulk submission
     *
     * If content was set via {@link PostDataRequest#setContent(BytesReference)}, then simply that content is returned.
     *
     * @throws IOException on parsing/serialization errors
     */
    public BytesReference getContent() throws IOException {
        if (content != null) {
            return content;
        }
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
            for (BytesReference bytesReference : bytesReferences) {
                try (StreamInput streamInput = bytesReference.streamInput()) {
                    builder.rawValue(streamInput, xContentType);
                }
            }
            for (Map<String, Object> objectMap : objectMaps) {
                builder.map(objectMap);
            }
            return BytesReference.bytes(builder);

    }

    public XContentType getXContentType() {
        return xContentType;
    }

    /**
     * Set the total content to post.
     *
     * @param content BytesReference content to set, format must match the set XContentType
     */
    public void setContent(BytesReference content) {
        this.content = content;
    }

    /**
     * Add a document via a ByteReference.
     *
     * Ignored if total content is set via {@link PostDataRequest#setContent(BytesReference)}
     *
     * @param bytesReference document to add to bulk request, format must match the set XContentType
     */
    public void addDoc(BytesReference bytesReference) {
        this.bytesReferences.add(Objects.requireNonNull(bytesReference, "bytesReferences must not be null"));
    }

    /**
     * Add a document via an object map
     *
     * Ignored if total content is set via {@link PostDataRequest#setContent(BytesReference)}
     *
     * @param objectMap document object to add to bulk request
     */
    public void addDoc(Map<String, Object> objectMap) {
        this.objectMaps.add(Objects.requireNonNull(objectMap, "objectMap must not be null"));
    }

    @Override
    public int hashCode() {
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
        builder.field(RESET_END.getPreferredName(), resetEnd);
        builder.field(RESET_START.getPreferredName(), resetStart);
        builder.endObject();
        return builder;
    }
}
