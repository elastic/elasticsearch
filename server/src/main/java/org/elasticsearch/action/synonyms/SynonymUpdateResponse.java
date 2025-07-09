/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.synonyms.SynonymsManagementAPIService.SynonymsReloadResult;
import org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResultStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class SynonymUpdateResponse extends ActionResponse implements ToXContentObject {

    public static final String RESULT_FIELD = "result";
    public static final String RELOAD_ANALYZERS_DETAILS_FIELD = "reload_analyzers_details";
    static final ReloadAnalyzersResponse EMPTY_RELOAD_ANALYZER_RESPONSE = new ReloadAnalyzersResponse(0, 0, 0, List.of(), Map.of());

    private final UpdateSynonymsResultStatus updateStatus;
    private final ReloadAnalyzersResponse reloadAnalyzersResponse;

    public SynonymUpdateResponse(StreamInput in) throws IOException {
        this.updateStatus = in.readEnum(UpdateSynonymsResultStatus.class);
        if (in.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
            this.reloadAnalyzersResponse = in.readOptionalWriteable(ReloadAnalyzersResponse::new);
        } else {
            this.reloadAnalyzersResponse = new ReloadAnalyzersResponse(in);
        }
    }

    public SynonymUpdateResponse(SynonymsReloadResult synonymsReloadResult) {
        super();
        UpdateSynonymsResultStatus updateStatus = synonymsReloadResult.synonymsOperationResult();
        Objects.requireNonNull(updateStatus, "Update status must not be null");
        ReloadAnalyzersResponse reloadResponse = synonymsReloadResult.reloadAnalyzersResponse();

        this.updateStatus = updateStatus;
        this.reloadAnalyzersResponse = reloadResponse;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(RESULT_FIELD, updateStatus.name().toLowerCase(Locale.ENGLISH));
            if (reloadAnalyzersResponse != null) {
                builder.field(RELOAD_ANALYZERS_DETAILS_FIELD);
                reloadAnalyzersResponse.toXContent(builder, params);
            }
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(updateStatus);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
            out.writeOptionalWriteable(reloadAnalyzersResponse);
        } else {
            if (reloadAnalyzersResponse == null) {
                // Nulls will be written as empty reload analyzer responses for older versions
                EMPTY_RELOAD_ANALYZER_RESPONSE.writeTo(out);
            } else {
                reloadAnalyzersResponse.writeTo(out);
            }
        }
    }

    public RestStatus status() {
        return switch (updateStatus) {
            case CREATED -> RestStatus.CREATED;
            default -> RestStatus.OK;
        };
    }

    UpdateSynonymsResultStatus updateStatus() {
        return updateStatus;
    }

    ReloadAnalyzersResponse reloadAnalyzersResponse() {
        return reloadAnalyzersResponse;
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
