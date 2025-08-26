/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.util.Map;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SCALE_FIELD;

/**
 * Test utility for directly operating on a {@link ExponentialHistogram} backed by a JSON string,
 * which was created via {@link org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent}.
 */
public class JsonBackedExponentialHistogram implements ExponentialHistogram {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    private final int scale;

    private JsonBackedExponentialHistogram(int scale) {
        this.scale = scale;
    }

    @SuppressWarnings("unchecked")
    static ExponentialHistogram createFromJson(String json) {
        try {
            Map<?, ?> data = MAPPER.readValue(json, Map.class);
            if (data == null) {
                return null;
            }
            int scale = ((Number) data.get(SCALE_FIELD)).intValue();
            return new JsonBackedExponentialHistogram(scale);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse ExponentialHistogram from JSON", e);
        }
    }

    @Override
    public long ramBytesUsed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int scale() {
        return scale;
    }

    @Override
    public ZeroBucket zeroBucket() {
        return ZeroBucket.minimalEmpty();
    }

    @Override
    public Buckets positiveBuckets() {
        return ExponentialHistogram.empty().positiveBuckets();
    }

    @Override
    public Buckets negativeBuckets() {
        return ExponentialHistogram.empty().negativeBuckets();
    }

}
