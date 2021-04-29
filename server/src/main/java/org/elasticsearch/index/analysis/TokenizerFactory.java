/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;

import java.util.function.Supplier;

public interface TokenizerFactory {

    String name();

    Tokenizer create();

    static TokenizerFactory newFactory(String name, Supplier<Tokenizer> supplier) {
        return new TokenizerFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Tokenizer create() {
                return supplier.get();
            }
        };
    }
}
