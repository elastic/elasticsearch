/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.watcher.support.validation.Validation;

import java.io.IOException;

/**
 * The request to get the watch by name (id)
 */
public class GetWatchRequest extends MasterNodeReadRequest<GetWatchRequest> {

    private String id;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;


    public GetWatchRequest() {
    }

    /**
     * @param id name (id) of the watch to retrieve
     */
    public GetWatchRequest(String id) {
        this.id = id;
    }

    GetWatchRequest setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        Validation.Error error = Validation.watchId(id);
        if (error != null) {
            validationException = ValidateActions.addValidationError(error.message(), validationException);
        }
        return validationException;
    }


    /**
     * @return The name of the watch to retrieve
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public GetWatchRequest setVersion(long version) {
        this.version = version;
        return this;
    }

    public long getVersion() {
        return this.version;
    }

    public GetWatchRequest setVersionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType getVersionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        out.writeString(id);
    }

    @Override
    public String toString() {
        return "get [" + id +"]";
    }
}
