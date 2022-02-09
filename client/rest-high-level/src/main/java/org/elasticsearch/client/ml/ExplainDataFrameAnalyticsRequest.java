/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Request to explain the following about a data frame analytics job:
 * <ul>
 *     <li>field selection: which fields are included or are not in the analysis</li>
 *     <li>memory estimation: how much memory the job is estimated to require</li>
 * </ul>
 */
public class ExplainDataFrameAnalyticsRequest implements Validatable {

    private final String id;
    private final DataFrameAnalyticsConfig config;

    public ExplainDataFrameAnalyticsRequest(String id) {
        this.id = Objects.requireNonNull(id);
        this.config = null;
    }

    public ExplainDataFrameAnalyticsRequest(DataFrameAnalyticsConfig config) {
        this.id = null;
        this.config = Objects.requireNonNull(config);
    }

    @Nullable
    public String getId() {
        return id;
    }

    @Nullable
    public DataFrameAnalyticsConfig getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExplainDataFrameAnalyticsRequest other = (ExplainDataFrameAnalyticsRequest) o;
        return Objects.equals(id, other.id) && Objects.equals(config, other.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, config);
    }
}
