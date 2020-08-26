/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.Nullable;

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
