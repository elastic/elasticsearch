/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.aggregatemetric.AggregateMetricFeatureSetUsage;

import java.util.Map;

public class AggregateMetricFeatureSet implements XPackFeatureSet {

    @Override
    public String name() {
        return XPackField.AGGREGATE_METRIC;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        listener.onResponse(new AggregateMetricFeatureSetUsage());
    }
}
