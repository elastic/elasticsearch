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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.StringBuilders;

public class DeprecatedMessage extends ParameterizedMessage implements LoggerMessage {
    private String xOpaqueId;

    public DeprecatedMessage(String messagePattern, String xOpaqueId, Object... args) {
        super(messagePattern, args);
        this.xOpaqueId = xOpaqueId;
    }

    @Override
    public String getFormat() {
        return "JSON_FORMATTED";
    }

    @Override
    public String getFormattedMessage() {
        String formattedMessage = super.getFormattedMessage();
        StringBuilder escapedMessage = new StringBuilder(formattedMessage);
        StringBuilders.escapeJson(escapedMessage, 0);

        StringBuilder escapedXOpaqueId = new StringBuilder(xOpaqueId);
        StringBuilders.escapeJson(escapedXOpaqueId, 0);

        return "\"message\": \"" + escapedMessage + "\", \"x-opaque-id\": \"" + escapedXOpaqueId + "\"";
    }
}
