/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.transform;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.Objects;

/**
 * The Transform Input allows to configure a transformation, that should be
 * put between two other inputs in a chained input in order to support easy
 * data transformations
 *
 * This class does not have a builder, as it just consists of a single
 * transform
 */
public class TransformInput implements Input {

    public static final String TYPE = "transform";

    private final Transform transform;

    public TransformInput(Transform transform) {
        this.transform = transform;
    }

    public Transform getTransform() {
        return transform;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field(transform.type(), transform, params).endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return transform.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransformInput that = (TransformInput) o;

        return Objects.equals(transform, that.transform);
    }

    static class Result extends Input.Result {

        Result(Payload payload) {
            super(TYPE, payload);
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }
}
