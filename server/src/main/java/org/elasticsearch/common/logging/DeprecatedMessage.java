/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressLoggerChecks;

import java.util.Locale;

/**
 * A logger message used by {@link DeprecationLogger}, enriched with fields
 * named following ECS conventions. Carries x-opaque-id field if provided in the headers.
 * Will populate the x-opaque-id field in JSON logs.
 */
public class DeprecatedMessage {
    public static final String ELASTIC_ORIGIN_FIELD_NAME = "elasticsearch.elastic_product_origin";
    public static final String KEY_FIELD_NAME = "event.code";
    public static final String X_OPAQUE_ID_FIELD_NAME = "elasticsearch.http.request.x_opaque_id";
    public static final String ECS_VERSION = "1.2.0";

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    public static ESLogMessage of(
        DeprecationCategory category,
        String key,
        String xOpaqueId,
        String productOrigin,
        String messagePattern,
        Object... args
    ) {
        return getEsLogMessage(category, key, xOpaqueId, productOrigin, messagePattern, args);
    }

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    public static ESLogMessage compatibleDeprecationMessage(
        String key,
        String xOpaqueId,
        String productOrigin,
        String messagePattern,
        Object... args
    ) {
        return getEsLogMessage(DeprecationCategory.COMPATIBLE_API, key, xOpaqueId, productOrigin, messagePattern, args);
    }

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    private static ESLogMessage getEsLogMessage(
        DeprecationCategory category,
        String key,
        String xOpaqueId,
        String productOrigin,
        String messagePattern,
        Object[] args
    ) {
        ESLogMessage esLogMessage = new ESLogMessage(messagePattern, args).field("data_stream.dataset", "deprecation.elasticsearch")
            .field("data_stream.type", "logs")
            .field("data_stream.namespace", "default")
            .field(KEY_FIELD_NAME, key)
            .field("elasticsearch.event.category", category.name().toLowerCase(Locale.ROOT));

        if (Strings.isNullOrEmpty(xOpaqueId) == false) {
            esLogMessage.field(X_OPAQUE_ID_FIELD_NAME, xOpaqueId);
        }
        if (Strings.isNullOrEmpty(productOrigin) == false) {
            esLogMessage.field(ELASTIC_ORIGIN_FIELD_NAME, productOrigin);
        }
        return esLogMessage;
    }
}
