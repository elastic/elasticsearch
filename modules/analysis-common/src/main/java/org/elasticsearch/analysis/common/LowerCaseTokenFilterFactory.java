/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.el.GreekLowerCaseFilter;
import org.apache.lucene.analysis.ga.IrishLowerCaseFilter;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

/**
 * Factory for {@link LowerCaseFilter} and some language-specific variants
 * supported by the {@code language} parameter:
 * <ul>
 *   <li>greek: {@link GreekLowerCaseFilter}
 *   <li>irish: {@link IrishLowerCaseFilter}
 *   <li>turkish: {@link TurkishLowerCaseFilter}
 * </ul>
 */
public class LowerCaseTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final String lang;

    LowerCaseTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.lang = settings.get("language", null);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (lang == null) {
            return new LowerCaseFilter(tokenStream);
        } else if (lang.equalsIgnoreCase("greek")) {
            return new GreekLowerCaseFilter(tokenStream);
        } else if (lang.equalsIgnoreCase("irish")) {
            return new IrishLowerCaseFilter(tokenStream);
        } else if (lang.equalsIgnoreCase("turkish")) {
            return new TurkishLowerCaseFilter(tokenStream);
        } else {
            throw new IllegalArgumentException("language [" + lang + "] not support for lower case");
        }
    }

}


