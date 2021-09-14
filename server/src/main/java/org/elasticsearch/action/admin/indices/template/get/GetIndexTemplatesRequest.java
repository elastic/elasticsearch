/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request that allows to retrieve index templates
 */
public class GetIndexTemplatesRequest extends MasterNodeReadRequest<GetIndexTemplatesRequest> {

    private String[] names;

    public GetIndexTemplatesRequest() {
    }

    public GetIndexTemplatesRequest(String... names) {
        this.names = names;
    }

    public GetIndexTemplatesRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null) {
            validationException = addValidationError("names is null or empty", validationException);
        } else {
            for (String name : names) {
                if (name == null || Strings.hasText(name) == false) {
                    validationException = addValidationError("name is missing", validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Sets the names of the index templates.
     */
    public GetIndexTemplatesRequest names(String... names) {
        this.names = names;
        return this;
    }

    /**
     * The names of the index templates.
     */
    public String[] names() {
        return this.names;
    }
}
