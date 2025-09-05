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
    public static ExponentialHistogram createFromJson(String json) {
        try {
            Map<String, ?> data = MAPPER.readValue(json, Map.class);
            return createFromMap(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse ExponentialHistogram from JSON", e);
        }
    }

    public static ExponentialHistogram createFromMap(Map<String, ?> parsedJson) {
        if (parsedJson == null) {
            return null;
        }
        int scale = ((Number) parsedJson.get(SCALE_FIELD)).intValue();
        return new JsonBackedExponentialHistogram(scale);
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
    public double sum() {
        // TODO: implement
        return 0;
    }

    @Override
    public long valueCount() {
        // TODO: implement
        return 0;
    }

    @Override
    public double min() {
        // TODO: implement
        return Double.NaN;
    }

    @Override
    public double max() {
        // TODO: implement
        return Double.NaN;
    }

    @Override
    public ZeroBucket zeroBucket() {
        // TODO: implement
        return ZeroBucket.minimalEmpty();
    }

    @Override
    public Buckets positiveBuckets() {
        // TODO: implement
        return ExponentialHistogram.empty().positiveBuckets();
    }

    @Override
    public Buckets negativeBuckets() {
        // TODO: implement
        return ExponentialHistogram.empty().negativeBuckets();
    }

}
