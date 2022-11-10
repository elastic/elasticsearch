/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.example.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.example.analysis.lucene.SkipTokenFilter;
import org.elasticsearch.example.analysis.lucene.UnderscoreTokenizer;
import org.elasticsearch.plugin.api.NamedComponent;

@NamedComponent( "example_analyzer_factory")
public class ExampleAnalyzerFactory implements org.elasticsearch.plugin.analysis.api.AnalyzerFactory {

    @Override
    //TODO guide lucene
    public Analyzer create() {
        return new CustomAnalyzer();
    }

    static class CustomAnalyzer extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            var tokenizer = new UnderscoreTokenizer();
            var tokenFilter = new SkipTokenFilter(tokenizer, 1L);
            return new TokenStreamComponents(r -> tokenizer.setReader(new ReplaceCharToNumber(r, "#", 3)), tokenFilter);
        }
    }
}

