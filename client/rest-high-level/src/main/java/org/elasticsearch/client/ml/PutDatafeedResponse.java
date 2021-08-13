/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the newly created {@link DatafeedConfig}
 */
public class PutDatafeedResponse implements ToXContentObject {

    private DatafeedConfig datafeed;

    public static PutDatafeedResponse fromXContent(XContentParser parser) throws IOException {
        return new PutDatafeedResponse(DatafeedConfig.PARSER.parse(parser, null).build());
    }

    PutDatafeedResponse(DatafeedConfig datafeed) {
        this.datafeed = datafeed;
    }

    public DatafeedConfig getResponse() {
        return datafeed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        datafeed.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PutDatafeedResponse response = (PutDatafeedResponse) object;
        return Objects.equals(datafeed, response.datafeed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeed);
    }
}
