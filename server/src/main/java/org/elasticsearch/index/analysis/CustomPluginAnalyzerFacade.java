/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.util.ArrayList;
import java.util.List;

public class CustomPluginAnalyzerFacade {

    private final String name;

    public CustomPluginAnalyzerFacade(String name) {
        this.name = name;
    }

    public List<AnalyzeAction.AnalyzeToken> simpleAnalyze() {
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();


        return tokens;
    }
}
