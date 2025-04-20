/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete an index template.
 */
public class DeleteIndexTemplateRequest extends MasterNodeRequest<DeleteIndexTemplateRequest> {

    private String name;

    public DeleteIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
    }

    public DeleteIndexTemplateRequest() {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
    }

    /**
     * Constructs a new delete index request for the specified name.
     */
    public DeleteIndexTemplateRequest(String name) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
        this.name = name;
    }

    /**
     * Set the index template name to delete.
     */
    public DeleteIndexTemplateRequest name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    /**
     * The index template name to delete.
     */
    public String name() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
