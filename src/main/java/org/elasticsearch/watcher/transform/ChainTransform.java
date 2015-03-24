/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.support.init.InitializingService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class ChainTransform extends Transform<ChainTransform.Result> {

    public static final String TYPE = "chain";

    private final ImmutableList<Transform> transforms;

    public ChainTransform(ImmutableList<Transform> transforms) {
        this.transforms = transforms;
    }

    @Override
    public String type() {
        return TYPE;
    }

    ImmutableList<Transform> transforms() {
        return transforms;
    }

    @Override
    public Result apply(WatchExecutionContext ctx, Payload payload) throws IOException {
        ImmutableList.Builder<Transform.Result> results = ImmutableList.builder();
        for (Transform transform : transforms) {
            Transform.Result result = transform.apply(ctx, payload);
            results.add(result);
            payload = result.payload();
        }
        return new Result(TYPE, payload, results.build());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Transform transform : transforms) {
            builder.startObject()
                    .field(transform.type(), transform)
                    .endObject();
        }
        return builder.endArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChainTransform transform = (ChainTransform) o;

        if (!transforms.equals(transform.transforms)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return transforms.hashCode();
    }

    public static class Result extends Transform.Result {

        private final List<Transform.Result> results;

        public Result(String type, Payload payload, List<Transform.Result> results) {
            super(type, payload);
            this.results = results;
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(Parser.RESULTS_FIELD.getPreferredName());
            for (Transform.Result result : results) {
                builder.startObject()
                        .field(result.type(), result)
                        .endObject();
            }
            return builder.endArray();
        }
    }

    public static class Parser implements Transform.Parser<Result, ChainTransform>, InitializingService.Initializable {

        public static final ParseField RESULTS_FIELD = new ParseField("results");

        private TransformRegistry registry;

        // used by guice
        public Parser() {
        }

        // used for tests
        Parser(TransformRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void init(Injector injector) {
            init(injector.getInstance(TransformRegistry.class));
        }

        public void init(TransformRegistry registry) {
            this.registry = registry;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ChainTransform parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new WatcherSettingsException("could not parse [chain] transform. expected an array of objects, but found [" + token + '}');
            }

            ImmutableList.Builder<Transform> builder = ImmutableList.builder();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new WatcherSettingsException("could not parse [chain] transform. expected a transform object, but found [" + token + "]");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        builder.add(registry.parse(currentFieldName, parser));
                    } else {
                        throw new WatcherSettingsException("could not parse [chain] transform. expected a transform object, but found [" + token + "]");
                    }
                }
            }
            return new ChainTransform(builder.build());
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new TransformException("could not parse [chain] transform result. expected an object, but found [" + token + "]");
            }

            Payload payload = null;
            ImmutableList.Builder<Transform.Result> results = ImmutableList.builder();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (token == XContentParser.Token.START_OBJECT) {
                        if (PAYLOAD_FIELD.match(currentFieldName)) {
                            payload = new Payload.XContent(parser);
                        } else {
                            throw new TransformException("could not parse [chain] transform result. unexpected object field [" + currentFieldName + "]");
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (RESULTS_FIELD.match(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.START_OBJECT) {
                                    results.add(registry.parseResult(parser));
                                } else {
                                    throw new TransformException("could not parse [chain] transform result. expected an object representing a transform result, but found [" + token + "]");
                                }
                            }
                        } else {
                            throw new TransformException("could not parse [chain] transform result. unexpected array field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new TransformException("could not parse [chain] transform result. unexpected token [" + token+ "]");
                    }
                }
            }
            return new Result(TYPE, payload, results.build());
        }
    }

    public static class SourceBuilder implements Transform.SourceBuilder {

        private final ImmutableList.Builder<Transform.SourceBuilder> builders = ImmutableList.builder();

        @Override
        public String type() {
            return TYPE;
        }

        public SourceBuilder(Transform.SourceBuilder... builders) {
            this.builders.add(builders);
        }

        public SourceBuilder add(Transform.SourceBuilder builder) {
            builders.add(builder);
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (Transform.SourceBuilder transBuilder : builders.build()) {
                builder.startObject()
                        .field(TYPE, transBuilder)
                        .endObject();
            }
            return builder.endArray();
        }
    }

}
