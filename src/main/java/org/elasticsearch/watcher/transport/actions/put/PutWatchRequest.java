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

    private String name;
    private BytesReference source;
    private boolean sourceUnsafe;

    public PutWatchRequest() {
    }

    public PutWatchRequest(String name, WatchSourceBuilder source) {
        this(name, source.buildAsBytes(XContentType.JSON), false);
    }

    public PutWatchRequest(String name, BytesReference source, boolean sourceUnsafe) {
        this.name = name;
        this.source = source;
        this.sourceUnsafe = sourceUnsafe;
    }

    /**
     * @return The name that will be the ID of the indexed document
     */
    public String getName() {
        return name;
    }

    /**
     * Set the watch name
     */
    public void setName(String name) {
        this.name = name;
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
    public void source(WatchSourceBuilder source) {
        source(source.buildAsBytes(XContentType.JSON));
    }

    /**
     * Set the source of the watch
     */
    public void source(BytesReference source) {
        this.source = source;
        this.sourceUnsafe = false;
    }

    /**
     * Set the source of the watch with boolean to control source safety
     */
    public void source(BytesReference source, boolean sourceUnsafe) {
        this.source = source;
        this.sourceUnsafe = sourceUnsafe;
    }

    public void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
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
        name = in.readString();
        source = in.readBytesReference();
        sourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeBytesReference(source);
    }

}
