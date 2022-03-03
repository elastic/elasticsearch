/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetBasicStatusResponse extends ActionResponse implements ToXContentObject {

    private boolean eligibleToStartBasic;

    GetBasicStatusResponse(StreamInput in) throws IOException {
        super(in);
        eligibleToStartBasic = in.readBoolean();
    }

    public GetBasicStatusResponse(boolean eligibleToStartBasic) {
        this.eligibleToStartBasic = eligibleToStartBasic;
    }

    boolean isEligibleToStartBasic() {
        return eligibleToStartBasic;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(eligibleToStartBasic);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("eligible_to_start_basic", eligibleToStartBasic);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetBasicStatusResponse that = (GetBasicStatusResponse) o;
        return eligibleToStartBasic == that.eligibleToStartBasic;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eligibleToStartBasic);
    }
}
