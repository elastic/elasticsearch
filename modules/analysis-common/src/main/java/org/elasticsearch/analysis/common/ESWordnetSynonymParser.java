/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

import java.io.IOException;

public class ESWordnetSynonymParser extends WordnetSynonymParser {
    private static final Logger logger = LogManager.getLogger(ESWordnetSynonymParser.class);

    private final boolean lenient;

    public ESWordnetSynonymParser(boolean dedup, boolean expand, boolean lenient, Analyzer analyzer) {
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
