/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.client.security.support.ServiceTokenInfo;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;

public class ServiceAccountCredentialsNodesResponse {

    private final NodesResponseHeader header;
    private final List<ServiceTokenInfo> fileTokenInfos;

    public ServiceAccountCredentialsNodesResponse(NodesResponseHeader header, List<ServiceTokenInfo> fileTokenInfos) {
        this.header = header;
        this.fileTokenInfos = fileTokenInfos;
    }

    public NodesResponseHeader getHeader() {
        return header;
    }

    public List<ServiceTokenInfo> getFileTokenInfos() {
        return fileTokenInfos;
    }

    public static ServiceAccountCredentialsNodesResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        NodesResponseHeader header = null;
        List<ServiceTokenInfo> fileTokenInfos = List.of();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            if ("_nodes".equals(parser.currentName())) {
                if (header == null) {
                    header = NodesResponseHeader.fromXContent(parser, null);
                } else {
                    throw new IllegalArgumentException("expecting only a single [_nodes] field, multiple found");
                }
            } else if ("file_tokens".equals(parser.currentName())) {
                fileTokenInfos = parseFileToken(parser);
            } else {
                throw new IllegalArgumentException(
                    "expecting field of either [_nodes] or [file_tokens], found [" + parser.currentName() + "]"
                );
            }
        }
        return new ServiceAccountCredentialsNodesResponse(header, fileTokenInfos);
    }

    static List<ServiceTokenInfo> parseFileToken(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        final ArrayList<ServiceTokenInfo> fileTokenInfos = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            final String tokenName = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), "nodes");
            parser.nextToken();
            final List<String> nodeNames = XContentParserUtils.parseList(parser, XContentParser::text);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            fileTokenInfos.add(new ServiceTokenInfo(tokenName, "file", nodeNames));
        }
        return fileTokenInfos;
    }
}
