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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.KEY_SETTINGS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * A subset of {@link IndexMetadata} storing only the shard count of an index
 * Prior to v9.3, the entire {@link IndexMetadata} object was stored in heap and then loaded during snapshotting to determine
 * the shard count. As per ES-12539, this is replaced with the {@link IndexShardCount} class that writes and loads only the index's
 * shard count to and from heap memory, reducing the possibility of smaller nodes going OOMe during snapshotting
 */
public record IndexShardCount(int count) {
    /**
     * Parses an {@link IndexMetadata} object, reading only the shard count and skipping the rest
     * @param parser The parser of the {@link IndexMetadata} object
     * @return Returns an {@link IndexShardCount} containing the shard count for the index
     * @throws IOException Thrown if the {@link IndexMetadata} object cannot be parsed correctly
     */
    public static IndexShardCount fromIndexMetaData(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName;
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        IndexShardCount indexShardCount = new IndexShardCount(-1);
        // Skip over everything except the settings object we care about, or any unexpected tokens
        while ((currentFieldName = parser.nextFieldName()) != null) {
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName.equals(KEY_SETTINGS)) {
                    Settings settings = Settings.fromXContent(parser);
                    indexShardCount = new IndexShardCount(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1));
                } else {
                    // Iterate through the object, but we don't care for it's contents
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                // Iterate through the array, but we don't care for it's contents
                parser.skipChildren();
            } else if (token.isValue() == false) {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return indexShardCount;
    }
}
