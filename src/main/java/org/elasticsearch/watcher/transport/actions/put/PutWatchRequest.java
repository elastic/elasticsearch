/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.put;


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * This request class contains the data needed to create a watch along with the name of the watch.
 * The name of the watch will become the ID of the indexed document.
 */
public class PutWatchRequest extends MasterNodeOperationRequest<PutWatchRequest> {

    private String id;
    private BytesReference source;
    private boolean sourceUnsafe;

    PutWatchRequest() {
    }

    public PutWatchRequest(String id, WatchSourceBuilder source) {
        this(id, source.buildAsBytes(XContentType.JSON), false);
    }

    public PutWatchRequest(String id, BytesReference source, boolean sourceUnsafe) {
        this.id = id;
        this.source = source;
        this.sourceUnsafe = sourceUnsafe;
    }

    /**
     * @return The name that will be the ID of the indexed document
     */
    public String getId() {
        return id;
    }

    /**
     * Set the watch name
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The source of the watch
     */
    public BytesReference getSource() {
        return source;
    }

    /**
     * Set the source of the watch
     */
    public void setSource(WatchSourceBuilder source) {
        setSource(source.buildAsBytes(XContentType.JSON));
    }

    /**
     * Set the source of the watch
     */
    public void setSource(BytesReference source) {
        this.source = source;
        this.sourceUnsafe = false;
    }

    /**
     * Set the source of the watch with boolean to control source safety
     */
    public void setSource(BytesReference source, boolean sourceUnsafe) {
        this.source = source;
        this.sourceUnsafe = sourceUnsafe;
    }

    void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = ValidateActions.addValidationError("watch name is missing", validationException);
        }
        if (source == null) {
            validationException = ValidateActions.addValidationError("watch source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        source = in.readBytesReference();
        sourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBytesReference(source);
    }

}
