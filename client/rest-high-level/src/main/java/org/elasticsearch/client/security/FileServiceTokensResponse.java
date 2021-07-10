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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;

public class FileServiceTokensResponse {

    private final NodesResponseHeader header;
    private final List<ServiceTokenInfo> tokenInfos;

    public FileServiceTokensResponse(
        NodesResponseHeader header, List<ServiceTokenInfo> tokenInfos) {
        this.header = header;
        this.tokenInfos = tokenInfos;
    }

    public NodesResponseHeader getHeader() {
        return header;
    }

    public List<ServiceTokenInfo> getTokenInfos() {
        return tokenInfos;
    }

    public static FileServiceTokensResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        NodesResponseHeader header = null;
        final ArrayList<ServiceTokenInfo> fileTokenInfos = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            if ("_nodes".equals(parser.currentName())) {
                if (header == null) {
                    header = NodesResponseHeader.fromXContent(parser, null);
                } else {
                    throw new IllegalArgumentException("multiple [_nodes] fields found for the [file_tokens] section");
                }
            } else {
                fileTokenInfos.add(parseFileToken(parser.currentName(), parser));
            }
        }
        return new FileServiceTokensResponse(header, fileTokenInfos);
    }

    static ServiceTokenInfo parseFileToken(String tokenName, XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ensureFieldName(parser, parser.nextToken(), "nodes");
        parser.nextToken();
        final List<String> nodeNames = XContentParserUtils.parseList(parser, XContentParser::text);
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new ServiceTokenInfo(tokenName, "file", nodeNames);
    }
}
