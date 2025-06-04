/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Response with error information for a request to add/remove aliases for one or more indices.
 * Contains an acknowledged boolean, an errors boolean, and a list of results.
 * The result list is only present if there are errors, and contains a result for every input action.
 * This response replaces AcknowledgedResponse, and knows how to de/serialize from/to AcknowledgedResponse
 * in case of mixed version clusters.
 */
public class IndicesAliasesResponse extends AcknowledgedResponse {

    // Response without any error information, analogous to AcknowledgedResponse.FALSE
    public static final IndicesAliasesResponse NOT_ACKNOWLEDGED = new IndicesAliasesResponse(false, false, List.of());

    // Response without any error information, analogous to AcknowledgedResponse.TRUE
    public static final IndicesAliasesResponse ACKNOWLEDGED_NO_ERRORS = new IndicesAliasesResponse(true, false, List.of());

    private static final String ACTION_RESULTS_FIELD = "action_results";
    private static final String ERRORS_FIELD = "errors";

    private final List<AliasActionResult> actionResults;
    private final boolean errors;

    protected IndicesAliasesResponse(StreamInput in) throws IOException {
        super(in);

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.errors = in.readBoolean();
            this.actionResults = in.readCollectionAsImmutableList(AliasActionResult::new);
        } else {
            this.errors = false;
            this.actionResults = List.of();
        }
    }

    /**
     * @param acknowledged  whether the update was acknowledged by all the relevant nodes in the cluster
     * @param errors true if any of the requested actions failed
     * @param actionResults the list of results for each input action, only present if there are errors
     */
    IndicesAliasesResponse(boolean acknowledged, boolean errors, final List<AliasActionResult> actionResults) {
        super(acknowledged);
        this.errors = errors;
        this.actionResults = actionResults;
    }

    public List<AliasActionResult> getActionResults() {
        return actionResults;
    }

    public boolean hasErrors() {
        return errors;
    }

    /**
     * Get a list of all errors from the response. If there are no errors, an empty list is returned.
     */
    public List<ElasticsearchException> getErrors() {
        if (errors == false) {
            return List.of();
        } else {
            return actionResults.stream().filter(a -> a.getError() != null).map(AliasActionResult::getError).toList();
        }
    }

    /**
     *  Build a response from a list of action results. Sets the errors boolean based
     *  on whether an of the individual results contain an error.
     * @param actionResults an action result for each of the requested alias actions
     * @return response containing all action results
     */
    public static IndicesAliasesResponse build(final List<AliasActionResult> actionResults) {
        assert actionResults.isEmpty() == false : "IndicesAliasesResponse must be instantiated with at least one action result.";
        final boolean errors = actionResults.stream().anyMatch(a -> a.error != null);
        return new IndicesAliasesResponse(true, errors, actionResults);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
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

    @Override
    // Only used equals in tests
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        IndicesAliasesResponse response = (IndicesAliasesResponse) o;
        return errors == response.errors && Objects.equals(actionResults, response.actionResults);
    }

    @Override
    // Only used hashCode in tests
    public int hashCode() {
        return Objects.hash(super.hashCode(), actionResults, errors);
    }

    /**
     * Result for a single alias add/remove action
     */
    public static class AliasActionResult implements Writeable, ToXContentObject {

        /**
         * Resolved indices to which the action applies. This duplicates information
         * which exists in the action, but is included because the action indices may
         * or may not be resolved depending on if the security layer is used or not.
         */
        private final List<String> indices;
        private final AliasActions action;
        private final ElasticsearchException error;

        /**
         * Build result that could be either a success or failure
         * @param indices the resolved indices to which the associated action applies
         * @param action the alias action consisting of add/remove, aliases, and indices
         * @param numAliasesRemoved the number of aliases remove, if any
         * @return the action result
         */
        public static AliasActionResult build(List<String> indices, AliasActions action, int numAliasesRemoved) {
            if (action.actionType() == AliasActions.Type.REMOVE && numAliasesRemoved == 0) {
                return buildRemoveError(indices, action);
            }
            return buildSuccess(indices, action);
        }

        /**
         * Build an error result for a failed remove action.
         */
        private static AliasActionResult buildRemoveError(List<String> indices, AliasActions action) {
            return new AliasActionResult(indices, action, new AliasesNotFoundException((action.getOriginalAliases())));
        }

        /**
         * Build a success action result with no errors.
         */
        public static AliasActionResult buildSuccess(List<String> indices, AliasActions action) {
            return new AliasActionResult(indices, action, null);
        }

        /**
         * The error result if the action failed, null if the action succeeded.
         */
        public ElasticsearchException getError() {
            return error;
        }

        private int getStatus() {
            return error == null ? 200 : error.status().getStatus();
        }

        private AliasActionResult(List<String> indices, AliasActions action, ElasticsearchException error) {
            assert indices.isEmpty() == false : "Alias action result must be instantiated with at least one index";
            this.indices = indices;
            this.action = action;
            this.error = error;
        }

        private AliasActionResult(StreamInput in) throws IOException {
            this.indices = in.readStringCollectionAsList();
            this.action = new AliasActions(in);
            this.error = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
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
            builder.field(ACTION_INDICES_FIELD, indices.stream().sorted().collect(Collectors.toList()));
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

        @Override
        // Only used equals in tests
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AliasActionResult that = (AliasActionResult) o;
            return Objects.equals(indices, that.indices) && Objects.equals(action, that.action)
            // ElasticsearchException does not have hashCode() so assume errors are equal iff class and message are equal
                && Objects.equals(error == null ? null : error.getMessage(), that.error == null ? null : that.error.getMessage())
                && Objects.equals(error == null ? null : error.getClass(), that.error == null ? null : that.error.getClass());
        }

        @Override
        // Only used hashCode in tests
        public int hashCode() {
            return Objects.hash(
                indices,
                action,
                // ElasticsearchException does not have hashCode() so assume errors are equal iff class and message are equal
                error == null ? null : error.getMessage(),
                error == null ? null : error.getClass()
            );
        }
    }
}
