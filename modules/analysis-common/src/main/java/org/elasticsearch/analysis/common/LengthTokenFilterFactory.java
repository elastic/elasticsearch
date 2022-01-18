/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class LengthTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int min;
    private final int max;

    // ancient unsupported option
    private static final String ENABLE_POS_INC_KEY = "enable_position_increments";

    LengthTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        min = settings.getAsInt("min", 0);
        max = settings.getAsInt("max", Integer.MAX_VALUE);
        if (settings.get(ENABLE_POS_INC_KEY) != null) {
            throw new IllegalArgumentException(ENABLE_POS_INC_KEY + " is not supported anymore. Please fix your analysis chain");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new LengthFilter(tokenStream, min, max);
    }
}
