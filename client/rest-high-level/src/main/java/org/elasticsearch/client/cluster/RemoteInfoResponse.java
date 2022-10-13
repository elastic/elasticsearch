/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.cluster;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response to _remote/info API request.
 */
public final class RemoteInfoResponse {

    private List<RemoteConnectionInfo> infos;

    RemoteInfoResponse(Collection<RemoteConnectionInfo> infos) {
        this.infos = List.copyOf(infos);
    }

    public List<RemoteConnectionInfo> getInfos() {
        return infos;
    }

    public static RemoteInfoResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        List<RemoteConnectionInfo> infos = new ArrayList<>();

        XContentParser.Token token;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            String clusterAlias = parser.currentName();
            RemoteConnectionInfo info = RemoteConnectionInfo.fromXContent(parser, clusterAlias);
            infos.add(info);
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);
        return new RemoteInfoResponse(infos);
    }
}
