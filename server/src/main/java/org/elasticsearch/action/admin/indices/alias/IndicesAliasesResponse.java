/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
        final boolean errors = actionResults.stream().anyMatch(a -> a.success == false);
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
        public static AliasActionResult REMOVE_MISSING = new AliasActionResult(
            AliasActions.Type.REMOVE,
            false,
            "aliases_not_found_exception"
        );

        private final AliasActions.Type actionType;
        private final boolean success;
        private final String error;

        boolean getSuccess() {
            return success;
        }

        public static AliasActionResult merge(AliasActionResult currResult, AliasActionResult newResult) {
            // there is not currently a result value for this action, so return new value
            if (currResult == null) {
                return newResult;
            }

            // current value is error due to removal of missing alias, replace with newResult
            if (currResult.success == false) {
                return newResult;
            }

            // return current value the action has been successful
            return currResult;
        }

        public static AliasActionResult buildSuccess(AliasActions action) {
            return new AliasActionResult(action.actionType(), true, null);
        }

        private AliasActionResult(AliasActions.Type actionType, boolean success, String error) {
            assert error != null ^ success : "AliasActionResult should contain error message if and only if action not successful";
            this.actionType = actionType;
            this.success = success;
            this.error = error;
        }

        private AliasActionResult(StreamInput in) throws IOException {
            this.actionType = AliasActions.Type.fromValue(in.readByte());
            this.success = in.readBoolean();
            this.error = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(actionType.value());
            out.writeBoolean(success);
            out.writeOptionalString(error);
        }

        public static final String ACTION_FIELD = "action";
        public static final String SUCCESS_FIELD = "success";
        public static final String ERROR_FIELD = "error";

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACTION_FIELD, actionType.getFieldName());
            builder.field(SUCCESS_FIELD, success);
            if (error != null) {
                builder.field(ERROR_FIELD, error);
            }
            builder.endObject();
            return builder;
        }
    }
}
