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

package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class LegacyDelimitedPayloadTokenFilterFactory extends DelimitedPayloadTokenFilterFactory {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(LegacyDelimitedPayloadTokenFilterFactory.class));

    LegacyDelimitedPayloadTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException(
                    "[delimited_payload_filter] is not supported for new indices, use [delimited_payload] instead");
        }
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_2_0)) {
            deprecationLogger.deprecated("Deprecated [delimited_payload_filter] used, replaced by [delimited_payload]");
        }
    }
}
