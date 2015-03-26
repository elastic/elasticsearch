package org.elasticsearch.search.aggregations.reducers.smooth.models;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;


public interface SmoothingModelBuilder {
    public void toXContent(XContentBuilder builder) throws IOException;
}
