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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.QueryPage;
import org.elasticsearch.xpack.ml.job.results.PageParams;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class GetModelSnapshotsAction
extends Action<GetModelSnapshotsAction.Request, GetModelSnapshotsAction.Response, GetModelSnapshotsAction.RequestBuilder> {

    public static final GetModelSnapshotsAction INSTANCE = new GetModelSnapshotsAction();
    public static final String NAME = "cluster:admin/ml/model_snapshots/get";

    private GetModelSnapshotsAction() {
        super(NAME);
    }

    @Override
    public GetModelSnapshotsAction.RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public GetModelSnapshotsAction.Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {

        public static final ParseField SORT = new ParseField("sort");
        public static final ParseField DESCRIPTION = new ParseField("description");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField DESC = new ParseField("desc");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString(Request::setDescriptionString, DESCRIPTION);
            PARSER.declareString(Request::setStart, START);
            PARSER.declareString(Request::setEnd, END);
            PARSER.declareString(Request::setSort, SORT);
            PARSER.declareBoolean(Request::setDescOrder, DESC);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private String sort;
        private String description;
        private String start;
        private String end;
        private boolean desc;
        private PageParams pageParams = new PageParams();

        Request() {
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        @Nullable
        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = sort;
        }

        public boolean getDescOrder() {
            return desc;
        }

        public void setDescOrder(boolean desc) {
            this.desc = desc;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = ExceptionsHelper.requireNonNull(pageParams, PageParams.PAGE.getPreferredName());
        }

        @Nullable
        public String getStart() {
            return start;
        }

        public void setStart(String start) {
            this.start = ExceptionsHelper.requireNonNull(start, START.getPreferredName());
        }

        @Nullable
        public String getEnd() {
            return end;
        }

        public void setEnd(String end) {
            this.end = ExceptionsHelper.requireNonNull(end, END.getPreferredName());
        }

        @Nullable
        public String getDescriptionString() {
            return description;
        }

        public void setDescriptionString(String description) {
            this.description = ExceptionsHelper.requireNonNull(description, DESCRIPTION.getPreferredName());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            sort = in.readOptionalString();
            description = in.readOptionalString();
            start = in.readOptionalString();
            end = in.readOptionalString();
            desc = in.readBoolean();
            pageParams = new PageParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeOptionalString(sort);
            out.writeOptionalString(description);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeBoolean(desc);
            pageParams.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (description != null) {
                builder.field(DESCRIPTION.getPreferredName(), description);
            }
            if (start != null) {
                builder.field(START.getPreferredName(), start);
            }
            if (end != null) {
                builder.field(END.getPreferredName(), end);
            }
            if (sort != null) {
                builder.field(SORT.getPreferredName(), sort);
            }
            builder.field(DESC.getPreferredName(), desc);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, description, start, end, sort, desc);
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
            return Objects.equals(jobId, other.jobId) && Objects.equals(description, other.description)
                    && Objects.equals(start, other.start) && Objects.equals(end, other.end) && Objects.equals(sort, other.sort)
                    && Objects.equals(desc, other.desc);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private QueryPage<ModelSnapshot> page;

        public Response(QueryPage<ModelSnapshot> page) {
            this.page = page;
        }

        Response() {
        }

        public QueryPage<ModelSnapshot> getPage() {
            return page;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            page = new QueryPage<>(in, ModelSnapshot::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            page.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            page.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(page);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(page, other.page);
        }

        @SuppressWarnings("deprecation")
        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (Exception e) {
                // So we have a stack trace logged somewhere
                return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetModelSnapshotsAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final JobProvider jobProvider;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver, JobProvider jobProvider) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.jobProvider = jobProvider;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            logger.debug(String.format(Locale.ROOT,
                    "Get model snapshots for job %s. from = %d, size = %d"
                            + " start = '%s', end='%s', sort=%s descending=%b, description filter=%s",
                    request.getJobId(), request.pageParams.getFrom(), request.pageParams.getSize(), request.getStart(), request.getEnd(),
                    request.getSort(), request.getDescOrder(), request.getDescriptionString()));

            jobProvider.modelSnapshots(request.getJobId(), request.pageParams.getFrom(), request.pageParams.getSize(),
                    request.getStart(), request.getEnd(), request.getSort(), request.getDescOrder(), null, request.getDescriptionString(),
                    page -> {
                        clearQuantiles(page);
                        listener.onResponse(new Response(page));
                    }, listener::onFailure);
        }

        public static void clearQuantiles(QueryPage<ModelSnapshot> page) {
            if (page.results() != null) {
                for (ModelSnapshot modelSnapshot : page.results()) {
                    modelSnapshot.setQuantiles(null);
                }
            }
        }
    }

}
