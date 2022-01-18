/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

/**
 * Real work actually done here by Sebastian on the Elasticsearch mailing list
 * http://elasticsearch-users.115913.n3.nabble.com/Using-the-Snowball-stemmers-tp2126106p2127111.html
 */
public class SnowballTokenFilterFactory extends AbstractTokenFilterFactory {

    private String language;

    SnowballTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.language = Strings.capitalize(settings.get("language", settings.get("name", "English")));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new SnowballFilter(tokenStream, language);
    }

}
