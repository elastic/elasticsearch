/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.KoreanNumberFilter;
import org.elasticsearch.sp.api.analysis.TokenFilterFactory;
import org.elasticsearch.sp.api.analysis.settings.Inject;

public class NoriNumberFilterFactory2 implements TokenFilterFactory {

    private String name;

    @Inject
    public NoriNumberFilterFactory2() {}

    @Inject
    public NoriNumberFilterFactory2(NoriAnalysisSettings noriAnalysisSettings) {
        // System.out.println("new nori " + noriAnalysisSettings.getDecompoundMode());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new KoreanNumberFilter(tokenStream);
    }
}
