/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transform.chain;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ChainTransform implements Transform {

    public static final String TYPE = "chain";

    private final List<Transform> transforms;

    public ChainTransform(Transform... transforms) {
        this(Arrays.asList(transforms));
    }

    public ChainTransform(List<Transform> transforms) {
        this.transforms = Collections.unmodifiableList(transforms);
    }

    @Override
    public String type() {
        return TYPE;
    }

    public List<Transform> getTransforms() {
        return transforms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChainTransform that = (ChainTransform) o;

        return transforms.equals(that.transforms);
    }

    @Override
    public int hashCode() {
        return transforms.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Transform transform : transforms) {
            builder.startObject().field(transform.type(), transform, params).endObject();
        }
        return builder.endArray();
    }

    static ChainTransform parse(String watchId, XContentParser parser, TransformRegistry transformRegistry) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException(
                "could not parse [{}] transform for watch [{}]. expected an array of transform objects," + " but found [{}] instead",
                TYPE,
                watchId,
                token
            );
        }

        List<Transform> transforms = new ArrayList<>();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException(
                    "could not parse [{}] transform for watch [{}]. expected a transform object, but " + "found [{}] instead",
                    TYPE,
                    watchId,
                    token
                );
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    transforms.add(transformRegistry.parseTransform(watchId, currentFieldName, parser));
                }
            }
        }
        return new ChainTransform(transforms);
    }

    public static Builder builder(Transform... transforms) {
        return new Builder(transforms);
    }

    public static class Result extends Transform.Result {

        private final List<Transform.Result> results;

        public Result(Payload payload, List<Transform.Result> results) {
            super(TYPE, payload);
            this.results = Collections.unmodifiableList(results);
        }

        public Result(Exception e, List<Transform.Result> results) {
            super(TYPE, e);
            this.results = Collections.unmodifiableList(results);
        }

        public Result(String errorMessage, List<Transform.Result> results) {
            super(TYPE, errorMessage);
            this.results = Collections.unmodifiableList(results);
        }

        public List<Transform.Result> results() {
            return results;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (results.isEmpty() == false) {
                builder.startObject(type);
                builder.startArray(Field.RESULTS.getPreferredName());
                for (Transform.Result result : results) {
                    result.toXContent(builder, params);
                }
                builder.endArray();
                builder.endObject();
            }
            return builder;
        }
    }

    public static final class Builder implements Transform.Builder<ChainTransform> {

        private final List<Transform> transforms = new ArrayList<>();

        public Builder(Transform... transforms) {
            add(transforms);
        }

        public Builder add(Transform... transformsToAdd) {
            Collections.addAll(this.transforms, transformsToAdd);
            return this;
        }

        public Builder add(Transform.Builder<?>... transformsToAdd) {
            for (Transform.Builder<?> transform : transformsToAdd) {
                this.transforms.add(transform.build());
            }
            return this;
        }

        @Override
        public ChainTransform build() {
            return new ChainTransform(transforms);
        }
    }

    interface Field {
        ParseField RESULTS = new ParseField("results");
    }
}
