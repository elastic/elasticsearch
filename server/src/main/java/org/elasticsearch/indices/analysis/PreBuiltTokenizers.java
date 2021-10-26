/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

public enum PreBuiltTokenizers {

    STANDARD(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new StandardTokenizer();
        }
    }

    ;

    protected abstract  Tokenizer create(Version version);

    protected TokenFilterFactory getMultiTermComponent(Version version) {
        return null;
    }

    private final CachingStrategy cachingStrategy;

    PreBuiltTokenizers(CachingStrategy cachingStrategy) {
        this.cachingStrategy = cachingStrategy;
    }

    public CachingStrategy getCachingStrategy() {
        return cachingStrategy;
    }
}
