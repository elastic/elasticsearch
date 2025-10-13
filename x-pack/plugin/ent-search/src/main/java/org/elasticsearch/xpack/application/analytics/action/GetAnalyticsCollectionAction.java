/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class GetAnalyticsCollectionAction {

    public static final String NAME = "cluster:admin/xpack/application/analytics/get";
    public static final ActionType<GetAnalyticsCollectionAction.Response> INSTANCE = new ActionType<>(NAME);

    private GetAnalyticsCollectionAction() {/* no instances */}

    public static class Request extends MasterNodeReadRequest<Request> implements ToXContentObject {
        private final String[] names;

        public static final ParseField NAMES_FIELD = new ParseField("names");

        public Request(TimeValue masterNodeTimeout, String[] names) {
            super(masterNodeTimeout);
            this.names = Objects.requireNonNull(names, "Collection names cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        public String[] getNames() {
            return this.names;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(this.names);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(this.names, request.names);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAMES_FIELD.getPreferredName(), names);
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final List<AnalyticsCollection> collections;

        public static final ParseField EVENT_DATA_STREAM_FIELD = new ParseField("event_data_stream");
        public static final ParseField EVENT_DATA_STREAM_NAME_FIELD = new ParseField("name");

        public Response(StreamInput in) throws IOException {
            this.collections = in.readCollectionAsList(AnalyticsCollection::new);
        }

        public Response(List<AnalyticsCollection> collections) {
            this.collections = collections;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            for (AnalyticsCollection collection : collections) {
                builder.startObject(collection.getName());
                {
                    builder.startObject(EVENT_DATA_STREAM_FIELD.getPreferredName());
                    builder.field(EVENT_DATA_STREAM_NAME_FIELD.getPreferredName(), collection.getEventDataStream());
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(collections);
        }

        public List<AnalyticsCollection> getAnalyticsCollections() {
            return collections;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.collections);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;

            return Objects.equals(this.collections, response.collections);
        }
    }
}
