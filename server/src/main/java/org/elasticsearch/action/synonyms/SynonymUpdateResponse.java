/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
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

    private final SynonymsManagementAPIService.UpdateSynonymsResultStatus updateStatus;
    private final ReloadAnalyzersResponse reloadAnalyzersResponse;

    public SynonymUpdateResponse(StreamInput in) throws IOException {
        super(in);
        this.updateStatus = in.readEnum(SynonymsManagementAPIService.UpdateSynonymsResultStatus.class);
        this.reloadAnalyzersResponse = new ReloadAnalyzersResponse(in);
    }

    public SynonymUpdateResponse(SynonymsManagementAPIService.UpdateSynonymsResult updateSynonymsResult) {
        super();
        SynonymsManagementAPIService.UpdateSynonymsResultStatus updateStatus = updateSynonymsResult.updateStatus();
        Objects.requireNonNull(updateStatus, "Update status must not be null");
        ReloadAnalyzersResponse reloadResponse = updateSynonymsResult.reloadAnalyzersResponse();
        Objects.requireNonNull(reloadResponse, "Reload analyzers response must not be null");

        this.updateStatus = updateStatus;
        this.reloadAnalyzersResponse = reloadResponse;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("result", updateStatus.name().toLowerCase(Locale.ENGLISH));
            builder.field("reload_analyzers_details");
            reloadAnalyzersResponse.toXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(updateStatus);
        reloadAnalyzersResponse.writeTo(out);
    }

    @Override
    public RestStatus status() {
        return switch (updateStatus) {
            case CREATED -> RestStatus.CREATED;
            default -> RestStatus.OK;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymUpdateResponse response = (SynonymUpdateResponse) o;
        return updateStatus == response.updateStatus && Objects.equals(reloadAnalyzersResponse, response.reloadAnalyzersResponse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateStatus, reloadAnalyzersResponse);
    }
}
