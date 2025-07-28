/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.io.IOException;

public class TotalHitsExtractor extends ConstantExtractor {

    public TotalHitsExtractor(Long constant) {
        super(constant);
    }

    TotalHitsExtractor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Object extract(MultiBucketsAggregation.Bucket bucket) {
        return validate(super.extract(bucket));
    }

    @Override
    public Object extract(SearchHit hit) {
        return validate(super.extract(hit));
    }

    private static Object validate(Object value) {
        if (Number.class.isInstance(value) == false) {
            throw new QlIllegalArgumentException(
                "Inconsistent total hits count handling, expected a numeric value but found a {}: {}",
                value == null ? null : value.getClass().getSimpleName(),
                value
            );
        }
        if (((Number) value).longValue() < 0) {
            throw new QlIllegalArgumentException(
                "Inconsistent total hits count handling, expected a non-negative value but found {}",
                value
            );
        }
        return value;
    }

}
