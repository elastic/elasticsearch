/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Action to mark an index to be force merged by updating its custom metadata.
 */
public class MarkIndexForDLMForceMergeAction {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:admin/dlm/mark_index_for_force_merge");
    public static final String DLM_INDEX_FOR_FORCE_MERGE_KEY = "dlm_index_for_force_merge";

    /**
     * Request to mark an index to be force merged.
     */
    public static class Request extends MasterNodeRequest<Request> {
        private final ProjectId projectId;
        private final String originalIndex;
        private final String indexToBeForceMerged;

        /**
         * Constructor for the request.
         * @param projectId the project id of the index
         * @param originalIndex the original index that is being transitioned through DLM lifecycle
         * @param indexToBeForceMerged the index that needs to be force merged
         */
        public Request(ProjectId projectId, String originalIndex, String indexToBeForceMerged) {
            super(INFINITE_MASTER_NODE_TIMEOUT);
            if (projectId == null) {
                throw new IllegalArgumentException("projectId must not be null or empty");
            }
            if (Strings.isNullOrEmpty(originalIndex)) {
                throw new IllegalArgumentException("originalIndex must not be null or empty");
            }
            if (Strings.isNullOrEmpty(indexToBeForceMerged)) {
                throw new IllegalArgumentException("indexToBeForceMerged must not be null or empty");
            }
            this.projectId = projectId;
            this.originalIndex = originalIndex;
            this.indexToBeForceMerged = indexToBeForceMerged;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
            this.originalIndex = in.readString();
            this.indexToBeForceMerged = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
            out.writeString(originalIndex);
            out.writeString(indexToBeForceMerged);
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public String getOriginalIndex() {
            return originalIndex;
        }

        public String getIndexToBeForceMerged() {
            return indexToBeForceMerged;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(projectId, originalIndex, indexToBeForceMerged);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals(projectId, request.projectId)
                && Objects.equals(originalIndex, request.originalIndex)
                && Objects.equals(indexToBeForceMerged, request.indexToBeForceMerged);
        }
    }
}
