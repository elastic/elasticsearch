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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Builder for the reload secure settings nodes request
 */
public class NodesReloadSecureSettingsRequestBuilder extends NodesOperationRequestBuilder<NodesReloadSecureSettingsRequest,
        NodesReloadSecureSettingsResponse, NodesReloadSecureSettingsRequestBuilder> {

    public static final String SECURE_SETTINGS_PASSWORD_FIELD_NAME = "secure_settings_password";

    public NodesReloadSecureSettingsRequestBuilder(ElasticsearchClient client, NodesReloadSecureSettingsAction action) {
        super(client, action, new NodesReloadSecureSettingsRequest());
    }

    public NodesReloadSecureSettingsRequestBuilder setSecureStorePassword(SecureString secureStorePassword) {
        request.secureStorePassword(secureStorePassword);
        return this;
    }

    public NodesReloadSecureSettingsRequestBuilder source(BytesReference source, XContentType xContentType) throws IOException {
        Objects.requireNonNull(xContentType);
        // EMPTY is ok here because we never call namedObject
        try (InputStream stream = source.streamInput();
                XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE, stream)) {
            XContentParser.Token token;
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected an object, but found token [{}]", token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME || false == SECURE_SETTINGS_PASSWORD_FIELD_NAME.equals(parser.currentName())) {
                throw new ElasticsearchParseException("expected a field named [{}], but found [{}]", SECURE_SETTINGS_PASSWORD_FIELD_NAME,
                        token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.VALUE_STRING) {
                throw new ElasticsearchParseException("expected field [{}] to be of type string, but found [{}] instead",
                        SECURE_SETTINGS_PASSWORD_FIELD_NAME, token);
            }
            final String password = parser.text();
            setSecureStorePassword(new SecureString(password.toCharArray()));
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ElasticsearchParseException("expected end of object, but found token [{}]", token);
            }
        }
        return this;
    }

}
