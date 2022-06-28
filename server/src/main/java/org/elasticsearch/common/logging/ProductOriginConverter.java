/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.tasks.Task;

import java.util.Objects;

/**
 * Pattern converter to format the X-elastic-product-origin into plaintext logs.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "ProductOriginConverter")
@ConverterKeys({ "product_origin" })
public final class ProductOriginConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ProductOriginConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ProductOriginConverter();
    }

    public ProductOriginConverter() {
        super("product_origin", "product_origin");
    }

    public static String getProductOrigin() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(t -> t.<String>getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    /**
     * Formats the X-elastic-product-origin into plaintext logs/
     *
     * @param event - a log event is ignored in this method as it uses the value from ThreadContext
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String productOrigin = getProductOrigin();
        if (productOrigin != null) {
            toAppendTo.append(productOrigin);
        }
    }
}
