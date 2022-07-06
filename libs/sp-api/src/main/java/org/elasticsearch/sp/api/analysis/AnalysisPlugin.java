/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.sp.api.analysis;

import java.util.Collections;
import java.util.Map;

public interface AnalysisPlugin {

    default Map<String, Class<? extends TokenFilterFactory>> getTokenFilterFactories() {
        return Collections.emptyMap();
    }

    default Map<String, Class<? extends TokenizerFactory>> getTokenizerFactories() {
        return Collections.emptyMap();
    }

    default Map<String, Class<? extends Analyzer>> getAnalyzers() {
        return Collections.emptyMap();
    }
}
