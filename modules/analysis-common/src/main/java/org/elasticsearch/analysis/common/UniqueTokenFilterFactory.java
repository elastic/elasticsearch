/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class UniqueTokenFilterFactory extends AbstractTokenFilterFactory {

    static final String ONLY_ON_SAME_POSITION = "only_on_same_position";
    private final boolean onlyOnSamePosition;
    private final boolean correctPositionIncrement;

    UniqueTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.onlyOnSamePosition = settings.getAsBoolean(ONLY_ON_SAME_POSITION, false);
        this.correctPositionIncrement = indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (correctPositionIncrement == false) {
            return new XUniqueTokenFilter(tokenStream, onlyOnSamePosition);
        }
        return new UniqueTokenFilter(tokenStream, onlyOnSamePosition);
    }
}
