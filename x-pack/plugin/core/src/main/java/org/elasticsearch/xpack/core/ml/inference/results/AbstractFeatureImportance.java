/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

abstract class AbstractFeatureImportance implements Writeable, ToXContentObject {

    public abstract String getFeatureName();

    public abstract Map<String, Object> toMap();

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(toMap());
    }
}
