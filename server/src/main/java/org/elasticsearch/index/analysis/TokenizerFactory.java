/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;

import java.util.function.Supplier;

public interface TokenizerFactory {

    String name();

    Tokenizer create();

    /**
     * See {@link TokenFilterFactory#sharingKey()}.
     */
    default Object sharingKey() {
        return this;
    }

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

            // Anonymous-factory created from a supplier — every call to newFactory yields a fresh
            // instance with a fresh supplier closure, so identity-equality (the supplier holds
            // unique captured state) is the correct sharing semantics.
            @Override
            public Object sharingKey() {
                return this;
            }
        };
    }
}
