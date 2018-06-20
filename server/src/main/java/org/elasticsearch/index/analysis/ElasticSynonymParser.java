/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;

public class ElasticSynonymParser extends SolrSynonymParser {

    private boolean lenient;
    private final Logger logger;

    public ElasticSynonymParser(boolean dedup, boolean expand, boolean lenient, Analyzer analyzer) {
        super(dedup, expand, analyzer);
        this.lenient = lenient;
        logger = Loggers.getLogger(getClass(), "ElasticSynonymParser");
    }

    @Override
    public void add(CharsRef input, CharsRef output, boolean includeOrig) {
        if (!lenient || (input.length > 0 && output.length > 0)) {
            super.add(input, output, includeOrig);
        } // the else would happen only in the case for lenient in which case we quietly ignore it
    }

    @Override
    public CharsRef analyze(String text, CharsRefBuilder reuse) throws IOException {
        try {
            return super.analyze(text, reuse);
        } catch (IllegalArgumentException ex) {
            if (lenient) {
                logger.info("Synonym rule for [" + text + "] was ignored");
                return new CharsRef("");
            } else {
                throw ex;
            }
        }
    }
}
