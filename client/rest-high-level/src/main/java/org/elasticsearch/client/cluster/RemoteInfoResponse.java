/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.cluster;

import org.elasticsearch.common.xcontent.XContentParser;

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
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        List<RemoteConnectionInfo> infos = new ArrayList<>();

        XContentParser.Token token;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            String clusterAlias = parser.currentName();
            RemoteConnectionInfo info = RemoteConnectionInfo.fromXContent(parser, clusterAlias);
            infos.add(info);
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser::getTokenLocation);
        return new RemoteInfoResponse(infos);
    }
}
