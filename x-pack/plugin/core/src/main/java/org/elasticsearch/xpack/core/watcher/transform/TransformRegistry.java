/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transform;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransform;
import org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransformFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TransformRegistry {

    private final Map<String, TransformFactory> factories;

    public TransformRegistry(Map<String, TransformFactory> factories) {
        Map<String, TransformFactory> map = new HashMap<>(factories);
        map.put(ChainTransform.TYPE, new ChainTransformFactory(this));
        this.factories = Collections.unmodifiableMap(map);
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

    private ExecutableTransform parse(String watchId, String type, XContentParser parser) throws IOException {
        TransformFactory factory = factories.get(type);
        if (factory == null) {
            throw new ElasticsearchParseException("could not parse transform for watch [{}], unknown transform type [{}]", watchId, type);
        }
        return factory.parseExecutable(watchId, parser);
    }

    public Transform parseTransform(String watchId, String type, XContentParser parser) throws IOException {
        TransformFactory factory = factories.get(type);
        if (factory == null) {
            throw new ElasticsearchParseException("could not parse transform for watch [{}], unknown transform type [{}]", watchId, type);
        }
        return factory.parseTransform(watchId, parser);
    }
}
