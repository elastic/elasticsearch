/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.chain;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class ChainTransform implements Transform {

    public static final String TYPE = "chain";

    private final ImmutableList<Transform> transforms;

    public ChainTransform(ImmutableList<Transform> transforms) {
        this.transforms = transforms;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public ImmutableList<Transform> getTransforms() {
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
            builder.startObject()
                    .field(transform.type(), transform, params)
                    .endObject();
        }
        return builder.endArray();
    }

    public static ChainTransform parse(String watchId, XContentParser parser, TransformRegistry transformRegistry) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new ChainTransformException("could not parse [{}] transform for watch [{}]. expected an array of transform objects, but found [{}] instead", TYPE, watchId, token);
        }

        ImmutableList.Builder<Transform> builder = ImmutableList.builder();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ChainTransformException("could not parse [{}] transform for watch [{}]. expected a transform object, but found [{}] instead", TYPE, watchId, token);
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    builder.add(transformRegistry.parseTransform(watchId, currentFieldName, parser));
                } else {
                    throw new ChainTransformException("could not parse [{}] transform for watch [{}]. expected a transform object, but found [{}] instead", TYPE, watchId, token);
                }
            }
        }
        return new ChainTransform(builder.build());
    }

    public static Builder builder(Transform... transforms) {
        return new Builder(transforms);
    }

    public static class Result extends Transform.Result {

        private final ImmutableList<Transform.Result> results;

        public Result(Payload payload, ImmutableList<Transform.Result> results) {
            super(TYPE, payload);
            this.results = results;
        }

        public ImmutableList<Transform.Result> results() {
            return results;
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(Field.RESULTS.getPreferredName());
            for (Transform.Result result : results) {
                builder.startObject()
                        .field(result.type(), result, params)
                        .endObject();
            }
            return builder.endArray();
        }

        public static Result parse(String watchId, XContentParser parser, TransformRegistry transformRegistry) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ChainTransformException("could not parse [{}] transform result for watch [{}]. expected an object, but found [{}] instead", TYPE, watchId, token);
            }

            Payload payload = null;
            ImmutableList.Builder<Transform.Result> results = ImmutableList.builder();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (token == XContentParser.Token.START_OBJECT) {
                        if (Field.PAYLOAD.match(currentFieldName)) {
                            payload = new Payload.XContent(parser);
                        } else {
                            throw new ChainTransformException("could not parse [{}] transform result for watch [{}]. unexpected object field [{}]", TYPE, watchId, currentFieldName);
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (Field.RESULTS.match(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.START_OBJECT) {
                                    results.add(transformRegistry.parseResult(watchId, parser));
                                } else {
                                    throw new ChainTransformException("could not parse [{}] transform result for watch [{}]. expected an object representing a transform result, but found [{}] instead", TYPE, watchId, token);
                                }
                            }
                        } else {
                            throw new ChainTransformException("could not parse [{}] transform result for watch [{}]. unexpected array field [{}]", TYPE, watchId, currentFieldName);
                        }
                    } else {
                        throw new ChainTransformException("could not parse [{}] transform result for watch [{}]. unexpected token [{}]", TYPE, watchId, token);
                    }
                }
            }
            return new ChainTransform.Result(payload, results.build());
        }
    }

    public static class Builder implements Transform.Builder<ChainTransform> {

        private final ImmutableList.Builder<Transform> transforms = ImmutableList.builder();

        public Builder(Transform... transforms) {
            this.transforms.add(transforms);
        }

        public Builder add(Transform... transforms) {
            this.transforms.add(transforms);
            return this;
        }

        public Builder add(Transform.Builder... transforms) {
            for (Transform.Builder transform: transforms) {
                this.transforms.add(transform.build());
            }
            return this;
        }

        @Override
        public ChainTransform build() {
            return new ChainTransform(transforms.build());
        }
    }

    interface Field extends Transform.Field {
        ParseField RESULTS = new ParseField("results");
    }
}
