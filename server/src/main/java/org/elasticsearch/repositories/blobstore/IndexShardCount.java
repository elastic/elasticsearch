/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.KEY_SETTINGS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * A subset of {@link IndexMetadata} storing only the shard count of an index
 */
public record IndexShardCount(int count) {
    /**
     * Parses an {@link IndexMetadata} object, reading only the shard count and skipping the rest.
     * Assumes that the settings object is flat, and not nested.
     * @param parser The parser of the {@link IndexMetadata} object
     * @return Returns an {@link IndexShardCount} containing the shard count for the index
     * @throws IOException Thrown if the {@link IndexMetadata} object cannot be parsed correctly
     */
    public static IndexShardCount fromIndexMetadata(XContentParser parser) throws IOException {
        parser.nextToken(); // fresh parser so move to the first token
        parser.nextToken(); // on a start object move to next token
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName;
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        IndexShardCount indexShardCount = null;
        // Skip over everything except the index.number_of_shards setting, or any unexpected tokens
        while ((currentFieldName = parser.nextFieldName()) != null) {
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName.equals(KEY_SETTINGS)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        if (SETTING_NUMBER_OF_SHARDS.equals(fieldName)) {
                            assert indexShardCount == null : "number_of_shards setting encountered multiple times in index settings";
                            indexShardCount = new IndexShardCount(parser.intValue());
                        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                            // Settings should be flat, not nested
                            throw new IllegalArgumentException(
                                "Settings object contains nested object for key [" + fieldName + "], expected flat settings map"
                            );
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else if (token.isValue() == false) {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);

        // indexShardCount is null if corruption when parsing
        return indexShardCount != null ? indexShardCount : new IndexShardCount(-1);
    }
}
