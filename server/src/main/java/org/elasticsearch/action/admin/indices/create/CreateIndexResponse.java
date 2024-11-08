/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends ShardsAcknowledgedResponse {

    public static final ParseField INDEX = new ParseField("index");

    private final String index;

    public CreateIndexResponse(StreamInput in) throws IOException {
        super(in, true);
        index = in.readString();
    }

    public CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
        out.writeString(index);
    }

    public String index() {
        return index;
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(INDEX.getPreferredName(), index());
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            CreateIndexResponse that = (CreateIndexResponse) o;
            return Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}
