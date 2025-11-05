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

/**
 * Factory interface for creating tokenizers in the analysis chain.
 * Tokenizers break text into tokens and form the first stage of text analysis.
 */
public interface TokenizerFactory {

    /**
     * Retrieves the name of this tokenizer factory.
     *
     * @return the tokenizer name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TokenizerFactory factory = ...;
     * String name = factory.name(); // e.g., "standard"
     * }</pre>
     */
    String name();

    /**
     * Creates a new tokenizer instance.
     * Each call should return a new instance suitable for tokenizing a single document.
     *
     * @return a new Tokenizer instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TokenizerFactory factory = ...;
     * Tokenizer tokenizer = factory.create();
     * tokenizer.setReader(new StringReader("text to tokenize"));
     * }</pre>
     */
    Tokenizer create();

    /**
     * Creates a simple TokenizerFactory from a name and supplier.
     * Useful for creating lightweight tokenizer factories without implementing the full interface.
     *
     * @param name the name of the tokenizer
     * @param supplier a supplier that creates new Tokenizer instances
     * @return a new TokenizerFactory implementation
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TokenizerFactory factory = TokenizerFactory.newFactory(
     *     "custom",
     *     () -> new StandardTokenizer()
     * );
     * }</pre>
     */
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
