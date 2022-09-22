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
import org.elasticsearch.plugin.analysis.api.TokenFilterFactory;
import org.elasticsearch.plugin.api.NamedComponent;

@NamedComponent(name = "nori_number")
public class NoriNumberFilterFactory implements TokenFilterFactory {

    private String name;

    public NoriNumberFilterFactory() {
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
