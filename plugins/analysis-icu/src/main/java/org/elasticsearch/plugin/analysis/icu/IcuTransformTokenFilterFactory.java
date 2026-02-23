/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Transliterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUTransformFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

public class IcuTransformTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final Transliterator transliterator;

    public IcuTransformTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        String id = settings.get("id");
        String ruleset = settings.get("ruleset");
        String strDir = settings.get("dir", "forward");
        int dir = "forward".equals(strDir) ? Transliterator.FORWARD : Transliterator.REVERSE;

        if (id != null && ruleset != null) {
            throw new IllegalArgumentException("icu_transform filter [" + name + "] must not specify both [id] and [ruleset]");
        }

        if (ruleset != null) {
            try {
                this.transliterator = Transliterator.createFromRules(name, ruleset, dir);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("icu_transform filter [" + name + "] has invalid ruleset: " + e.getMessage(), e);
            }
        } else {
            this.transliterator = Transliterator.getInstance(id, dir);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ICUTransformFilter(tokenStream, transliterator);
    }

}
