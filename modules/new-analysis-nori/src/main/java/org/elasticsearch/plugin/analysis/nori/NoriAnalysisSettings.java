/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.elasticsearch.sp.api.analysis.settings.AnalysisSettings;
import org.elasticsearch.sp.api.analysis.settings.StringSetting;

@AnalysisSettings(prefix = "")
public interface NoriAnalysisSettings {

    @StringSetting(path = "decompoundMode")
    String getDecompoundMode();
}
