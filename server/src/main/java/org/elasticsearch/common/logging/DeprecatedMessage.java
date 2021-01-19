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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressLoggerChecks;

import java.util.Locale;

/**
 * A logger message used by {@link DeprecationLogger}.
 * Carries x-opaque-id field if provided in the headers. Will populate the x-opaque-id field in JSON logs.
 */
public class DeprecatedMessage  {
    public static final String X_OPAQUE_ID_FIELD_NAME = "x-opaque-id";
    public static final String ECS_VERSION = "1.6";

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    public static ESLogMessage of(DeprecationCategory category, String key, String xOpaqueId, String messagePattern, Object... args) {
        ESLogMessage esLogMessage = new ESLogMessage(messagePattern, args)
            .field("data_stream.type", "logs")
            .field("data_stream.dataset", "deprecation.elasticsearch")
            .field("data_stream.namespace", "default")
            .field("ecs.version", ECS_VERSION)
            .field("key", key)
            .field("elasticsearch.event.category", category.name().toLowerCase(Locale.ROOT));

        if (Strings.isNullOrEmpty(xOpaqueId)) {
            return esLogMessage;
        }

        return esLogMessage.field(X_OPAQUE_ID_FIELD_NAME, xOpaqueId);
    }
}
