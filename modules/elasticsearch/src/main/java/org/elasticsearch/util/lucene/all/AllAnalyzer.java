/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.lucene.all;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;

import java.io.IOException;
import java.io.Reader;

/**
 * An all analyzer.
 *
 * @author kimchy (shay.banon)
 */
public class AllAnalyzer extends Analyzer {

    private final Analyzer analyzer;

    public AllAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override public TokenStream tokenStream(String fieldName, Reader reader) {
        AllEntries allEntries = (AllEntries) reader;
        return new AllTokenFilter(analyzer.tokenStream(fieldName, reader), allEntries);
    }

    @Override public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        AllEntries allEntries = (AllEntries) reader;
        return new AllTokenFilter(analyzer.reusableTokenStream(fieldName, reader), allEntries);
    }

    @Override public int getPositionIncrementGap(String fieldName) {
        return analyzer.getPositionIncrementGap(fieldName);
    }

    @Override public int getOffsetGap(Fieldable field) {
        return analyzer.getOffsetGap(field);
    }
}
