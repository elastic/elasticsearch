/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PutDataFrameAnalyticsResponse {

    public static PutDataFrameAnalyticsResponse fromXContent(XContentParser parser) throws IOException {
        return new PutDataFrameAnalyticsResponse(DataFrameAnalyticsConfig.fromXContent(parser));
    }

    private final DataFrameAnalyticsConfig config;

    public PutDataFrameAnalyticsResponse(DataFrameAnalyticsConfig config) {
        this.config = config;
    }

    public DataFrameAnalyticsConfig getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PutDataFrameAnalyticsResponse other = (PutDataFrameAnalyticsResponse) o;
        return Objects.equals(config, other.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
