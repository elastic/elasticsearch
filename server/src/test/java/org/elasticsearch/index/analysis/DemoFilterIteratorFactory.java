/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.AbstractAnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.DelegatingTokenStream;
import org.elasticsearch.plugins.analysis.ESTokenStream;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;
import org.elasticsearch.plugins.analysis.StableLuceneFilterIterator;

public class DemoFilterIteratorFactory extends AbstractAnalysisIteratorFactory {

    public DemoFilterIteratorFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
    }

    @Override
    public PortableAnalyzeIterator newInstance(ESTokenStream esTokenStream) {
        return new StableLuceneFilterIterator(new ElasticWordOnlyTokenFilter(new DelegatingTokenStream(esTokenStream)));
    }

    private class ElasticWordOnlyTokenFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        ElasticWordOnlyTokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() {
            return termAtt.toString().equalsIgnoreCase("elastic");
        }
    }
}
