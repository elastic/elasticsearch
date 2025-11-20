/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.expectValueToken;

/**
 * A funnel that extracts dimensions from an {@link XContentParser} and adds them to a {@link TsidBuilder}.
 */
class XContentParserTsidFunnel implements TsidBuilder.ThrowingTsidFunnel<XContentParser, IOException> {

    private static final XContentParserTsidFunnel INSTANCE = new XContentParserTsidFunnel();

    static XContentParserTsidFunnel get() {
        return INSTANCE;
    }

    /**
     * Adds dimensions extracted from the provided {@link XContentParser} to the given {@link TsidBuilder}.
     * To only extract dimensions, the parser should be configured via
     * {@link XContentParserConfiguration#withFiltering(String, Set, Set, boolean)}.
     *
     * @param parser       the parser from which to read the JSON content
     * @param tsidBuilder  the builder to which dimensions will be added
     * @throws IOException if an error occurs while reading from the parser
     */
    @Override
    public void add(XContentParser parser, TsidBuilder tsidBuilder) throws IOException {
        ensureExpectedToken(null, parser.currentToken(), parser);
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Error extracting tsid: source didn't contain any dimension fields");
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        extractObject(tsidBuilder, null, parser);
        ensureExpectedToken(null, parser.nextToken(), parser);
    }

    private void extractObject(TsidBuilder tsidBuilder, @Nullable String path, XContentParser source) throws IOException {
        while (source.currentToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, source.currentToken(), source);
            String fieldName = source.currentName();
            String subPath = path == null ? fieldName : path + "." + fieldName;
            source.nextToken();
            extractItem(tsidBuilder, subPath, source);
        }
    }

    private void extractArray(TsidBuilder tsidBuilder, @Nullable String path, XContentParser source) throws IOException {
        while (source.currentToken() != XContentParser.Token.END_ARRAY) {
            expectValueToken(source.currentToken(), source);
            extractItem(tsidBuilder, path, source);
        }
    }

    private void extractItem(TsidBuilder tsidBuilder, String path, XContentParser source) throws IOException {
        switch (source.currentToken()) {
            case START_OBJECT:
                source.nextToken();
                extractObject(tsidBuilder, path, source);
                source.nextToken();
                break;
            case VALUE_NUMBER:
                switch (source.numberType()) {
                    case INT -> tsidBuilder.addIntDimension(path, source.intValue());
                    case LONG -> tsidBuilder.addLongDimension(path, source.longValue());
                    case FLOAT -> tsidBuilder.addDoubleDimension(path, source.floatValue());
                    case DOUBLE -> tsidBuilder.addDoubleDimension(path, source.doubleValue());
                    case BIG_DECIMAL, BIG_INTEGER -> tsidBuilder.addStringDimension(path, source.optimizedText().bytes());
                }
                source.nextToken();
                break;
            case VALUE_BOOLEAN:
                tsidBuilder.addBooleanDimension(path, source.booleanValue());
                source.nextToken();
                break;
            case VALUE_STRING:
                tsidBuilder.addStringDimension(path, source.optimizedText().bytes());
                source.nextToken();
                break;
            case START_ARRAY:
                source.nextToken();
                extractArray(tsidBuilder, path, source);
                source.nextToken();
                break;
            case VALUE_NULL:
                source.nextToken();
                break;
            default:
                throw new ParsingException(
                    source.getTokenLocation(),
                    "Cannot extract dimension due to unexpected token [{}]",
                    source.currentToken()
                );
        }
    }
}
