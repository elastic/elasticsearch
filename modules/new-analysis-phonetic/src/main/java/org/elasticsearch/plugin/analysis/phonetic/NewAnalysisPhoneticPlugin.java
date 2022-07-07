/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.elasticsearch.sp.api.analysis.AnalysisPlugin;
import org.elasticsearch.sp.api.analysis.TokenFilterFactory;

import java.util.Map;

import static java.util.Collections.singletonMap;

public class NewAnalysisPhoneticPlugin implements AnalysisPlugin {

    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilterFactories() {
        return singletonMap("phonetic2", PhoneticTokenFilterFactory.class);
    }
}
