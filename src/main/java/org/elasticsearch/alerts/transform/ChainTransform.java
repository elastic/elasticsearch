/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.init.InitializingService;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class ChainTransform extends Transform {

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
    public Result apply(ExecutionContext ctx, Payload payload) throws IOException {
        for (Transform transform : transforms) {
            payload = transform.apply(ctx, payload).payload();
        }
        return new Result(TYPE, payload);
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

    public static class Parser implements Transform.Parser<ChainTransform>, InitializingService.Initializable {

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
            this.registry = injector.getInstance(TransformRegistry.class);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ChainTransform parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new AlertsSettingsException("could not parse [chain] transform. expected an array of objects, but found [" + token + '}');
            }

            ImmutableList.Builder<Transform> builder = ImmutableList.builder();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new AlertsSettingsException("could not parse [chain] transform. expected a transform object, but found [" + token + "]");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        builder.add(registry.parse(currentFieldName, parser));
                    } else {
                        throw new AlertsSettingsException("could not parse [chain] transform. expected a transform object, but found [" + token + "]");
                    }
                }
            }
            return new ChainTransform(builder.build());
        }

    }


}
