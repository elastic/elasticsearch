/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;

/**
 * Interface for plan nodes that require the Configuration at parsing time.
 * <p>
 *     They should be created with {@link ConfigurationAware#CONFIGURATION_MARKER},
 *     and it will be resolved in the {@link org.elasticsearch.xpack.esql.analysis.Analyzer}.
 * </p>
 * <p>
 *   See <a href="https://github.com/elastic/elasticsearch/issues/138203">https://github.com/elastic/elasticsearch/issues/138203</a>
 * </p>
 */
public interface ConfigurationAware extends ConfigurationFunction {

    // Configuration placeholder used by the Analyzer to replace
    Configuration CONFIGURATION_MARKER = new Configuration(
        ZoneOffset.UTC,
        Locale.ROOT,
        StringUtils.EMPTY,
        StringUtils.EMPTY,
        QueryPragmas.EMPTY,
        0,
        0,
        StringUtils.EMPTY,
        false,
        Map.of(),
        0,
        false,
        0,
        0,
        null
    );

    Configuration configuration();

    Expression withConfiguration(Configuration configuration);
}
