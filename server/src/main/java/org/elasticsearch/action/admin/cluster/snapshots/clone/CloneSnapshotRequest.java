/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.clone;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CloneSnapshotRequest extends MasterNodeRequest<CloneSnapshotRequest> implements IndicesRequest.Replaceable, ToXContentObject {

    private final String repository;

    private final String source;

    private final String target;

    private String[] indices;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();

    public CloneSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        source = in.readString();
        target = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    /**
     * Creates a clone snapshot request for cloning the given source snapshot's indices into the given target snapshot on the given
     * repository.
     *
     * @param repository repository that source snapshot belongs to and that the target snapshot will be created in
     * @param source     source snapshot name
     * @param target     target snapshot name
     * @param indices    indices to clone from source to target
     */
    public CloneSnapshotRequest(String repository, String source, String target, String[] indices) {
        this.repository = repository;
        this.source = source;
        this.target = target;
        this.indices = indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(source);
        out.writeString(target);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("source snapshot name is missing", null);
        }
        if (target == null) {
            validationException = addValidationError("target snapshot name is missing", null);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else if (indices.length == 0) {
            validationException = addValidationError("indices patterns are empty", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public CloneSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * @see CloneSnapshotRequestBuilder#setIndicesOptions
     */
    public CloneSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public String repository() {
        return this.repository;
    }

    public String target() {
        return this.target;
    }

    public String source() {
        return this.source;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("source", source);
        builder.field("target", target);
        if (indices != null) {
            builder.startArray("indices");
            for (String index : indices) {
                builder.value(index);
            }
            builder.endArray();
        }
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
