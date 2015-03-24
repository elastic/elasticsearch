/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.watcher.watch.WatchStore;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;

import java.io.IOException;

/**
 * The request to get the watch by name (id)
 */
public class GetWatchRequest extends MasterNodeOperationRequest<GetWatchRequest> {

    private String watchName;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;


    public GetWatchRequest() {
    }

    /**
     * @param watchName name (id) of the watch to retrieve
     */
    public GetWatchRequest(String watchName) {
        this.watchName = watchName;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (watchName == null) {
            validationException = ValidateActions.addValidationError("watchName is missing", validationException);
        }
        return validationException;
    }


    /**
     * @return The name of the watch to retrieve
     */
    public String watchName() {
        return watchName;
    }

    public GetWatchRequest watchName(String watchName){
        this.watchName = watchName;
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public GetWatchRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public GetWatchRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        version = Versions.readVersion(in);
        versionType = VersionType.fromValue(in.readByte());
        watchName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Versions.writeVersion(version, out);
        out.writeByte(versionType.getValue());
        out.writeString(watchName);
    }

    @Override
    public String toString() {
        return "delete {[" + WatchStore.INDEX + "][" + watchName +"]}";
    }
}
