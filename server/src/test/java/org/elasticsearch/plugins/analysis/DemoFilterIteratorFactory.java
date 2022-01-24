/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.plugins.lucene.DelegatingTokenStream;
import org.elasticsearch.plugins.lucene.StableLuceneFilterIterator;

// Loaded by the separate class loader
public class DemoFilterIteratorFactory implements AnalysisIteratorFactory {
    public DemoFilterIteratorFactory() {}

    @Override
    public String name() {
        return "DEMO";
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
