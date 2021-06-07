/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;

public final class MlParserUtils {

    private MlParserUtils() {}

    /**
     * @param hit The search hit to parse
     * @param objectParser Parser for the object of type T
     * @return The parsed value of T from the search hit
     * @throws ElasticsearchException on failure
     */
    public static <T, U> T parse(SearchHit hit, BiFunction<XContentParser, U, T> objectParser) {
        BytesReference source = hit.getSourceRef();
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return objectParser.apply(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse " + hit.getId(), e);
        }
    }
}
