/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class IndicesAliasesResponse extends AcknowledgedResponse {

    public static final IndicesAliasesResponse NOT_ACKNOWLEDGED = new IndicesAliasesResponse(false, false, List.of());
    public static final IndicesAliasesResponse ACKNOWLEDGED_NO_ERRORS = new IndicesAliasesResponse(true, false, List.of());

    private static final String ACTION_RESULTS_FIELD = "action_results";
    private static final String ERRORS_FIELD = "errors";

    private final List<AliasActionResult> actionResults;
    private final boolean errors;

    protected IndicesAliasesResponse(StreamInput in) throws IOException {
        super(in);

        if (in.getTransportVersion().onOrAfter(TransportVersions.ALIAS_ACTION_RESULTS)) {
            this.errors = in.readBoolean();
            this.actionResults = in.readCollectionAsList(AliasActionResult::new);
        } else {
            this.errors = false;
            this.actionResults = List.of();
        }
    }

    private IndicesAliasesResponse(boolean acknowledged, boolean errors, final List<AliasActionResult> actionResults) {
        super(acknowledged);
        this.errors = errors;
        this.actionResults = actionResults;
    }

    public static IndicesAliasesResponse build(final List<AliasActionResult> actionResults) {
        final boolean errors = actionResults.stream().anyMatch(a -> a.error != null);
        return new IndicesAliasesResponse(true, errors, actionResults);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ALIAS_ACTION_RESULTS)) {
            out.writeBoolean(errors);
            out.writeCollection(actionResults);
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ERRORS_FIELD, errors);
        // if there are no errors, don't provide granular list of results
        if (errors) {
            builder.field(ACTION_RESULTS_FIELD, actionResults);
        }
    }

    public static class AliasActionResult implements Writeable, ToXContentObject {

        private final AliasActions action;
        private final ElasticsearchException error;

        public static AliasActionResult build(AliasActions action, int numAliasesRemoved) {
            if (action.actionType() == AliasActions.Type.REMOVE && numAliasesRemoved == 0) {
                return buildRemoveError(action);
            }
            return buildSuccess(action);
        }

        private static AliasActionResult buildRemoveError(AliasActions action) {
            return new AliasActionResult(action, new AliasesNotFoundException((action.getOriginalAliases())));
        }

        public static AliasActionResult buildSuccess(AliasActions action) {
            return new AliasActionResult(action, null);
        }

        private int getStatus() {
            return error == null ? 200 : error.status().getStatus();
        }

        private AliasActionResult(AliasActions action, ElasticsearchException error) {
            this.action = action;
            this.error = error;
        }

        private AliasActionResult(StreamInput in) throws IOException {
            this.action = new AliasActions(in);
            this.error = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            action.writeTo(out);
            out.writeException(error);
        }

        public static final String ACTION_FIELD = "action";
        public static final String ACTION_TYPE_FIELD = "type";
        public static final String ACTION_INDICES_FIELD = "indices";
        public static final String ACTION_ALIASES_FIELD = "aliases";
        public static final String STATUS_FIELD = "status";
        public static final String ERROR_FIELD = "error";

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            // include subset of fields from action request
            builder.field(ACTION_FIELD);
            builder.startObject();
            builder.field(ACTION_TYPE_FIELD, action.actionType().getFieldName());
            builder.array(ACTION_INDICES_FIELD, action.getOriginalIndices());
            builder.array(ACTION_ALIASES_FIELD, action.getOriginalAliases());
            builder.endObject();

            builder.field(STATUS_FIELD, getStatus());

            if (error != null) {
                builder.startObject(ERROR_FIELD);
                error.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }
}
