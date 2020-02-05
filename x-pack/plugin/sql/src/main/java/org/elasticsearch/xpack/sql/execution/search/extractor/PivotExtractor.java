/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;

import java.io.IOException;
import java.util.Objects;

public class PivotExtractor implements BucketExtractor {

    static final String NAME = "pv";

    private final BucketExtractor groupExtractor;
    private final BucketExtractor metricExtractor;
    private final Object value;

    public PivotExtractor(BucketExtractor groupExtractor, BucketExtractor metricExtractor, Object value) {
        this.groupExtractor = groupExtractor;
        this.metricExtractor = metricExtractor;
        this.value = value;
    }

    PivotExtractor(StreamInput in) throws IOException {
        groupExtractor = in.readNamedWriteable(BucketExtractor.class);
        metricExtractor = in.readNamedWriteable(BucketExtractor.class);
        value = in.readGenericValue();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(groupExtractor);
        out.writeNamedWriteable(metricExtractor);
        out.writeGenericValue(value);
    }

    @Override
    public Object extract(Bucket bucket) {
        if (Objects.equals(value, groupExtractor.extract(bucket))) {
            return metricExtractor.extract(bucket);
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupExtractor, metricExtractor, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        PivotExtractor other = (PivotExtractor) obj;
        return Objects.equals(groupExtractor, other.groupExtractor) 
                && Objects.equals(metricExtractor, other.metricExtractor)
                && Objects.equals(value, other.value);
    }
}