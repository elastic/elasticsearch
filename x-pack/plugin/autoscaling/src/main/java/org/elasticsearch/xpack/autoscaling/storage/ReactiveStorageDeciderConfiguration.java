/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderConfiguration;

import java.io.IOException;

public class ReactiveStorageDeciderConfiguration implements AutoscalingDeciderConfiguration {
    private static final ObjectParser<ReactiveStorageDeciderConfiguration, Void> PARSER;

    static {
        PARSER = new ObjectParser<>(ReactiveStorageDeciderService.NAME, ReactiveStorageDeciderConfiguration::new);
    }

    public static ReactiveStorageDeciderConfiguration parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ReactiveStorageDeciderConfiguration() {}

    public ReactiveStorageDeciderConfiguration(StreamInput in) throws IOException {
        this();
    }

    @Override
    public String name() {
        return ReactiveStorageDeciderService.NAME;
    }

    @Override
    public String getWriteableName() {
        return ReactiveStorageDeciderService.NAME;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }
}
