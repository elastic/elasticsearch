/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.SimplePatternSplitTokenizer;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.util.function.Supplier;

public class SimplePatternSplitTokenizerFactory extends AbstractTokenizerFactory {

    private final String pattern;

    private final Object sharingKey;

    // lazy so that tokenizers which are defined but not used by any analyzer are never compiled
    private final Supplier<Automaton> dfa;

    public SimplePatternSplitTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);

        pattern = settings.get("pattern", "");
        boolean legacyComplement = SimplePatternTokenizerFactory.legacyComplement(indexSettings);
        this.sharingKey = new Key(pattern, legacyComplement);
        this.dfa = CachedSupplier.wrap(() -> SimplePatternTokenizerFactory.compile(pattern, legacyComplement));
    }

    @Override
    public Tokenizer create() {
        return new SimplePatternSplitTokenizer(dfa.get());
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(String pattern, boolean legacyComplement) {}
}
