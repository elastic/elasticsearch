/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class UniqueTokenFilterFactory extends AbstractTokenFilterFactory {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(UniqueTokenFilterFactory.class);

    static final String ONLY_ON_SAME_POSITION = "only_on_same_position";
    static final String CORRECT_POSITION_INCREMENT = "correct_position_increment";
    private final boolean onlyOnSamePosition;
    private final boolean correctPositionIncrement;

    UniqueTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name, settings);
        this.onlyOnSamePosition = settings.getAsBoolean(ONLY_ON_SAME_POSITION, false);
        this.correctPositionIncrement = settings.getAsBoolean(
            CORRECT_POSITION_INCREMENT,
            indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX)
        );
        if (settings.hasValue(CORRECT_POSITION_INCREMENT)) {
            deprecationLogger.warn(
                DeprecationCategory.ANALYSIS,
                "unique_token_filter_position_fix",
                "The setting [correct_position_increment] is deprecated and enabled by default. It will be removed in a future version."
            );
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (correctPositionIncrement == false) {
            return new XUniqueTokenFilter(tokenStream, onlyOnSamePosition);
        }
        return new UniqueTokenFilter(tokenStream, onlyOnSamePosition);
    }
}
