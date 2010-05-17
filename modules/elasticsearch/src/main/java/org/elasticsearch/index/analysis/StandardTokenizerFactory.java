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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.inject.assistedinject.Assisted;
import org.elasticsearch.util.lucene.Lucene;
import org.elasticsearch.util.settings.Settings;

import java.io.Reader;

/**
 * @author kimchy (Shay Banon)
 */
public class StandardTokenizerFactory extends AbstractTokenizerFactory {

    private final int maxTokenLength;

    @Inject public StandardTokenizerFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        maxTokenLength = settings.getAsInt("max_token_length", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
    }

    @Override public Tokenizer create(Reader reader) {
        StandardTokenizer tokenizer = new StandardTokenizer(Lucene.ANALYZER_VERSION, reader);
        tokenizer.setMaxTokenLength(maxTokenLength);
        return tokenizer;
    }
}
