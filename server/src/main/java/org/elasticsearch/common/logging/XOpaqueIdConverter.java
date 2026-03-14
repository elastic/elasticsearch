/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

/**
 * Pattern converter to format the {@code X-Opaque-Id} request header into log fields.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "XOpaqueIdConverter")
@ConverterKeys({ "x_opaque_id" })
public final class XOpaqueIdConverter extends LogEventPatternConverter {

    public static XOpaqueIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new XOpaqueIdConverter();
    }

    public XOpaqueIdConverter() {
        super("x_opaque_id", "x_opaque_id");
    }

    public static String getXOpaqueId() {
        return HeaderWarning.getXOpaqueId();
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String xOpaqueId = getXOpaqueId();
        if (xOpaqueId != null) {
            toAppendTo.append(xOpaqueId);
        }
    }
}
