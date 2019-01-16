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
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to update information about an existing model snapshot for a given job
 */
public class UpdateModelSnapshotRequest extends ActionRequest implements ToXContentObject {


    public static final ConstructingObjectParser<UpdateModelSnapshotRequest, Void> PARSER = new ConstructingObjectParser<>(
        "update_model_snapshot_request", a -> new UpdateModelSnapshotRequest((String) a[0], (String) a[1]));


    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ModelSnapshot.SNAPSHOT_ID);
        PARSER.declareStringOrNull(UpdateModelSnapshotRequest::setDescription, ModelSnapshot.DESCRIPTION);
        PARSER.declareBoolean(UpdateModelSnapshotRequest::setRetain, ModelSnapshot.RETAIN);
    }

    private final String jobId;
    private String snapshotId;
    private String description;
    private Boolean retain;

    /**
     * Constructs a request to update information for a snapshot of given job
     * @param jobId id of the job from which to retrieve results
     * @param snapshotId id of the snapshot from which to retrieve results
     */
    public UpdateModelSnapshotRequest(String jobId, String snapshotId) {
        this.jobId = Objects.requireNonNull(jobId, "[" + Job.ID + "] must not be null");
        this.snapshotId = Objects.requireNonNull(snapshotId, "[" + ModelSnapshot.SNAPSHOT_ID + "] must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getDescription() {
        return description;
    }

    /**
     * The new description of the snapshot.
     * @param description the updated snapshot description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getRetain() {
        return retain;
    }

    /**
     * The new value of the "retain" property of the snapshot
     * @param retain the updated retain property
     */
    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId);
        if (description != null) {
            builder.field(ModelSnapshot.DESCRIPTION.getPreferredName(), description);
        }
        if (retain != null) {
            builder.field(ModelSnapshot.RETAIN.getPreferredName(), retain);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateModelSnapshotRequest request = (UpdateModelSnapshotRequest) obj;
        return Objects.equals(jobId, request.jobId)
            && Objects.equals(snapshotId, request.snapshotId)
            && Objects.equals(description, request.description)
            && Objects.equals(retain, request.retain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, snapshotId, description, retain);
    }
}
