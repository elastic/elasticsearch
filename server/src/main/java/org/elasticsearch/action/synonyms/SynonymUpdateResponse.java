/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class SynonymUpdateResponse extends ActionResponse implements StatusToXContentObject {

    private final SynonymsManagementAPIService.UpdateSynonymsResult result;

    public SynonymUpdateResponse(StreamInput in) throws IOException {
        super(in);
        this.result = in.readEnum((SynonymsManagementAPIService.UpdateSynonymsResult.class));
    }

    public SynonymUpdateResponse(SynonymsManagementAPIService.UpdateSynonymsResult result) {
        super();
        Objects.requireNonNull(result, "Result must not be null");
        this.result = result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("result", result.name().toLowerCase(Locale.ENGLISH));
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(result);
    }

    @Override
    public RestStatus status() {
        return switch (result) {
            case CREATED -> RestStatus.CREATED;
            default -> RestStatus.OK;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymUpdateResponse response = (SynonymUpdateResponse) o;
        return result == response.result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }
}
