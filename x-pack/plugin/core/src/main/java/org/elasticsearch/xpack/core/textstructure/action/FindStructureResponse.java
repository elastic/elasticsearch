/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.util.Objects;

public class FindStructureResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final TextStructure textStructure;

    public FindStructureResponse(TextStructure textStructure) {
        this.textStructure = textStructure;
    }

    FindStructureResponse(StreamInput in) throws IOException {
        textStructure = new TextStructure(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        textStructure.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        textStructure.toXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(textStructure);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        FindStructureResponse that = (FindStructureResponse) other;
        return Objects.equals(textStructure, that.textStructure);
    }
}
