/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TransformRegistry {

    private final ImmutableMap<String, TransformFactory> factories;

    @Inject
    public TransformRegistry(Map<String, TransformFactory> factories) {
        this.factories = ImmutableMap.copyOf(factories);
    }

    public TransformFactory factory(String type) {
        return factories.get(type);
    }

    public ExecutableTransform parse(String watchId, XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        ExecutableTransform transform = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type != null) {
                transform = parse(watchId, type, parser);
            }
        }
        return transform;
    }

    public ExecutableTransform parse(String watchId, String type, XContentParser parser) throws IOException {
        TransformFactory factory = factories.get(type);
        if (factory == null) {
            throw new TransformException("could not parse transform for watch [{}], unknown transform type [{}]", watchId, type);
        }
        return factory.parseExecutable(watchId, parser);
    }

    public Transform parseTransform(String watchId, String type, XContentParser parser) throws IOException {
        TransformFactory factory = factories.get(type);
        if (factory == null) {
            throw new TransformException("could not parse transform for watch [{}], unknown transform type [{}]", watchId, type);
        }
        return factory.parseTransform(watchId, parser);
    }

    public Transform.Result parseResult(String watchId, XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Transform.Result result = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type != null) {
                result = parseResult(watchId, type, parser);
            }
        }
        return result;
    }

    public Transform.Result parseResult(String watchId, String type, XContentParser parser) throws IOException {
        TransformFactory factory = factories.get(type);
        if (factory == null) {
            throw new TransformException("could not parse transform result for watch [{}]. unknown transform type [{}]", watchId, type);
        }
        return factory.parseResult(watchId, parser);
    }
}
