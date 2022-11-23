/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class UniqueTokenFilterFactory extends AbstractTokenFilterFactory {

    private final boolean onlyOnSamePosition;

    UniqueTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name, settings);
        this.onlyOnSamePosition = settings.getAsBoolean("only_on_same_position", false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new UniqueTokenFilter(tokenStream, onlyOnSamePosition);
    }
}
