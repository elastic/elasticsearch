/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class LegacyDelimitedPayloadTokenFilterFactory extends DelimitedPayloadTokenFilterFactory {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(LegacyDelimitedPayloadTokenFilterFactory.class);

    LegacyDelimitedPayloadTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException(
                "[delimited_payload_filter] is not supported for new indices, use [delimited_payload] instead"
            );
        }
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_2_0)) {
            deprecationLogger.critical(
                DeprecationCategory.ANALYSIS,
                "analysis_legacy_delimited_payload_filter",
                "Deprecated [delimited_payload_filter] used, replaced by [delimited_payload]"
            );
        }
    }
}
