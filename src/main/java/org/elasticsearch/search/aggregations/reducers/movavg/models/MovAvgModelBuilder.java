package org.elasticsearch.search.aggregations.reducers.movavg.models;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;


public interface MovAvgModelBuilder {
    public void toXContent(XContentBuilder builder) throws IOException;
}
