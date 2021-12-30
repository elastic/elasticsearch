/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.email.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.*;

import java.util.List;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.newAttributeFactory;

public class DemoTokenizerIteratorFactory extends AbstractAnalysisIteratorFactory {

    public DemoTokenizerIteratorFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
    }

    @Override
    public PortableAnalyzeIterator newInstance(ReaderProvider readerProvider) {
        return new StableLuceneTokenizerIterator(
            new UAX29URLEmailTokenizer(newAttributeFactory()),
            new AnalyzeState(-1, 0),
            readerProvider);
    }
}
