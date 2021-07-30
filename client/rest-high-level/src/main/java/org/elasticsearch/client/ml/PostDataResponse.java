/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response object when posting data to a Machine Learning Job
 */
public class PostDataResponse implements ToXContentObject {

    private DataCounts dataCounts;

    public static PostDataResponse fromXContent(XContentParser parser) throws IOException {
        return new PostDataResponse(DataCounts.PARSER.parse(parser, null));
    }

    public PostDataResponse(DataCounts counts) {
        this.dataCounts = counts;
    }

    public DataCounts getDataCounts() {
        return dataCounts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return dataCounts.toXContent(builder, params);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PostDataResponse other = (PostDataResponse) obj;
        return Objects.equals(dataCounts, other.dataCounts);
    }

}
