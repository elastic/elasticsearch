/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.tasks.Task;

/**
 * Pattern converter to format the x-elastic-product-origin header into JSON fields <code>elasticsearch.elastic_product_origin</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "ProductOriginConverter")
@ConverterKeys({ "product_origin" })
public final class ProductOriginConverter extends ThreadContextBasedConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ProductOriginConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ProductOriginConverter();
    }

    public ProductOriginConverter() {
        super("product_origin", Task.PRODUCT_ORIGIN);
    }
}
