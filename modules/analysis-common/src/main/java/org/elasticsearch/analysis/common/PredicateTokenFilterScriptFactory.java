/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;

/**
 * A factory for creating FilteringTokenFilters that determine whether or not to
 * accept their underlying token by consulting a script
 */
public class PredicateTokenFilterScriptFactory extends AbstractTokenFilterFactory {

    private final AnalysisPredicateScript.Factory factory;

    public PredicateTokenFilterScriptFactory(IndexSettings indexSettings, String name, Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);
        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ScriptFilteringTokenFilter(tokenStream, factory.newInstance());
    }

    private static class ScriptFilteringTokenFilter extends FilteringTokenFilter {

        final AnalysisPredicateScript script;
        final AnalysisPredicateScript.Token token;

        ScriptFilteringTokenFilter(TokenStream in, AnalysisPredicateScript script) {
            super(in);
            this.script = script;
            this.token = new AnalysisPredicateScript.Token(this);
        }

        @Override
        protected boolean accept() throws IOException {
            token.updatePosition();
            return script.execute(token);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            this.token.reset();
        }
    }
}
