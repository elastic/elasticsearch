/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * TODO: Add docs
 */
public interface OperatorHandler<T extends MasterNodeRequest<?>> {
    String CONTENT = "content";

    String key();

    TransformState transform(Object source, TransformState prevState) throws Exception;

    default Collection<String> dependencies() {
        return Collections.emptyList();
    }

    default void validate(T request) {
        ActionRequestValidationException exception = request.validate();
        if (exception != null) {
            throw new IllegalStateException("Validation error", exception);
        }
    }

    @SuppressWarnings("unchecked")
    default Map<String, ?> asMap(Object input) {
        if (input instanceof Map<?, ?> source) {
            return (Map<String, Object>) source;
        }
        throw new IllegalStateException("Unsupported " + key() + " request format");
    }

    default XContentParser mapToXContentParser(Map<String, ?> source) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.map(source);
            return XContentFactory.xContent(builder.contentType())
                .createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }
}
