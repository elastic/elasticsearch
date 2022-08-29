/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.analysis.plugin.api.TokenFilterFactory;
import org.elasticsearch.plugin.api.NamedComponent;

@NamedComponent(name = "xx")//TODO to be removed. test class to see this being picked up by server on startup.
public class XX implements TokenFilterFactory {
    @Override
    public String name() {
        return null;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return null;
    }
}
