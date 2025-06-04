/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
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
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                    .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                source,
                XContentType.JSON
            )
        ) {
            return objectParser.apply(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse " + hit.getId(), e);
        }
    }
}
