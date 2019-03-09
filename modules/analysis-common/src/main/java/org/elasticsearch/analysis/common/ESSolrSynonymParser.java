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

package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

import java.io.IOException;

public class ESSolrSynonymParser extends SolrSynonymParser {
    private static final Logger logger = LogManager.getLogger(ESSolrSynonymParser.class);

    private final boolean lenient;

    public ESSolrSynonymParser(boolean dedup, boolean expand, boolean lenient, Analyzer analyzer) {
        super(dedup, expand, analyzer);
        this.lenient = lenient;
    }

    @Override
    public void add(CharsRef input, CharsRef output, boolean includeOrig) {
        // This condition follows up on the overridden analyze method. In case lenient was set to true and there was an
        // exception during super.analyze we return a zero-length CharsRef for that word which caused an exception. When
        // the synonym mappings for the words are added using the add method we skip the ones that were left empty by
        // analyze i.e., in the case when lenient is set we only add those combinations which are non-zero-length. The
        // else would happen only in the case when the input or output is empty and lenient is set, in which case we
        // quietly ignore it. For more details on the control-flow see SolrSynonymParser::addInternal.
        if (lenient == false || (input.length > 0 && output.length > 0)) {
            super.add(input, output, includeOrig);
        }
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
