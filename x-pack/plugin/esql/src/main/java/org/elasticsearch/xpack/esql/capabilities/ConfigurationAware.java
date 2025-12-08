/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;

// See https://github.com/elastic/elasticsearch/issues/138203
public interface ConfigurationAware {

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
        0
    );

    Configuration configuration();

    <T> T withConfiguration(Configuration configuration);
}
