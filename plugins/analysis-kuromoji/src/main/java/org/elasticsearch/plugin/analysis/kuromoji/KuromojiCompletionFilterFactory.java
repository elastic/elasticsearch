/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter.Mode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class KuromojiCompletionFilterFactory extends AbstractTokenFilterFactory {

    private final Mode mode;

    public KuromojiCompletionFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        mode = getMode(settings);
    }

    public static JapaneseCompletionFilter.Mode getMode(Settings settings) {
        JapaneseCompletionFilter.Mode mode = Mode.INDEX;
        String modeSetting = settings.get("mode", null);
        if (modeSetting != null) {
            if ("index".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseCompletionFilter.Mode.INDEX;
            } else if ("query".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseCompletionFilter.Mode.QUERY;
            }
        }
        return mode;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new JapaneseCompletionFilter(tokenStream, mode);
    }

}
