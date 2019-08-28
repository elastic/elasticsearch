/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.datascience;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.datascience.action.DataScienceStatsAction;

import java.io.IOException;
import java.util.Objects;

public class DataScienceFeatureSetUsage extends XPackFeatureSet.Usage {

    private final DataScienceStatsAction.Response response;

    public DataScienceFeatureSetUsage(boolean available, boolean enabled, DataScienceStatsAction.Response response) {
        super(XPackField.DATA_SCIENCE, available, enabled);
        this.response = response;
    }

    public DataScienceFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.response = new DataScienceStatsAction.Response(input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, response);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (response != null) {
            response.toXContent(builder, params);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        response.writeTo(out);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataScienceFeatureSetUsage other = (DataScienceFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(response, other.response);
    }
}
