/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class NamedValuesContainer implements ToXContentObject, Writeable {

    private Map<String, Integer> container = new HashMap<>();

    public NamedValuesContainer() {}

    public NamedValuesContainer(final Map<String, Integer> valueMap) {
        container.putAll(valueMap);
    }

    public NamedValuesContainer(StreamInput input) throws IOException {
        this();
        container = input.readMap(StreamInput::readString, StreamInput::readInt);
    }

    public void add(final String key, final Integer value) {
        container.put(key, value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(container);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(container, StreamOutput::writeString, StreamOutput::writeInt);
    }
}
