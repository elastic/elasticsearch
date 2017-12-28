/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class UpdateCalendarJobAction extends Action<UpdateCalendarJobAction.Request, PutCalendarAction.Response,
        UpdateCalendarJobAction.RequestBuilder> {
    public static final UpdateCalendarJobAction INSTANCE = new UpdateCalendarJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/jobs/update";

    private UpdateCalendarJobAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public PutCalendarAction.Response newResponse() {
        return new PutCalendarAction.Response();
    }

    public static class Request extends ActionRequest {

        private String calendarId;
        private Set<String> jobIdsToAdd;
        private Set<String> jobIdsToRemove;

        Request() {
        }

        public Request(String calendarId, Set<String> jobIdsToAdd, Set<String> jobIdsToRemove) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.jobIdsToAdd = ExceptionsHelper.requireNonNull(jobIdsToAdd, "job_ids_to_add");
            this.jobIdsToRemove = ExceptionsHelper.requireNonNull(jobIdsToRemove, "job_ids_to_remove");
        }

        public String getCalendarId() {
            return calendarId;
        }

        public Set<String> getJobIdsToAdd() {
            return jobIdsToAdd;
        }

        public Set<String> getJobIdsToRemove() {
            return jobIdsToRemove;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendarId = in.readString();
            jobIdsToAdd = new HashSet<>(in.readList(StreamInput::readString));
            jobIdsToRemove = new HashSet<>(in.readList(StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeStringList(new ArrayList<>(jobIdsToAdd));
            out.writeStringList(new ArrayList<>(jobIdsToRemove));
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, jobIdsToAdd, jobIdsToRemove);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(calendarId, other.calendarId) && Objects.equals(jobIdsToAdd, other.jobIdsToAdd)
                    && Objects.equals(jobIdsToRemove, other.jobIdsToRemove);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, PutCalendarAction.Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

}

