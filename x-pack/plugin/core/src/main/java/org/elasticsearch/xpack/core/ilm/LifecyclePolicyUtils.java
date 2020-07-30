/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A utility class used for loading index lifecycle policies from the resource classpath
 */
public class LifecyclePolicyUtils {

    private LifecyclePolicyUtils() {};

    /**
     * Loads a built-in index lifecycle policy and returns its source.
     */
    public static LifecyclePolicy loadPolicy(String name, String resource, NamedXContentRegistry xContentRegistry) {
        try {
            BytesReference source = load(resource);
            validate(source);

            try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.THROW_UNSUPPORTED_OPERATION, source.utf8ToString())) {
                return LifecyclePolicy.parse(parser, name);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to load policy [" + name + "] from [" + resource + "]", e);
        }
    }

    /**
     * Loads a resource from the classpath and returns it as a {@link BytesReference}
     */
    private static BytesReference load(String name) throws IOException {
        try (InputStream is = LifecyclePolicyUtils.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
    }

    /**
     * Parses and validates that the source is not empty.
     */
    private static void validate(BytesReference source) {
        if (source == null) {
            throw new ElasticsearchParseException("policy must not be null");
        }

        try {
            XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("policy must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("invalid policy", e);
        }
    }
}
