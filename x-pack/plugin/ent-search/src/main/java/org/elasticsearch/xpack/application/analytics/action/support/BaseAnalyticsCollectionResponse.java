/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action.support;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Objects;

public class BaseAnalyticsCollectionResponse extends ActionResponse {
    protected final AnalyticsCollection analyticsCollection;

    public static final ParseField EVENT_DATA_STREAM = new ParseField("event_data_stream");
    public static final ParseField COLLECTION_NAME = new ParseField("name");

    public BaseAnalyticsCollectionResponse(StreamInput in) throws IOException {
        super(in);
        analyticsCollection = new AnalyticsCollection(in);
    }

    public BaseAnalyticsCollectionResponse(AnalyticsCollection analyticsCollection) {
        this.analyticsCollection = analyticsCollection;
    }

    public XContentBuilder toXContentCommon(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startObject(analyticsCollection.getName());
        builder.startObject(EVENT_DATA_STREAM.getPreferredName());
        builder.field(COLLECTION_NAME.getPreferredName(), analyticsCollection.getEventDataStream());
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseAnalyticsCollectionResponse that = (BaseAnalyticsCollectionResponse) o;
        return Objects.equals(this.analyticsCollection, that.analyticsCollection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyticsCollection);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.analyticsCollection.writeTo(out);
    }
}
