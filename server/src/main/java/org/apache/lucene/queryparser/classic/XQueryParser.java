/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.queryparser.classic;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

/**
 * This class is just a workaround to make {@link QueryParser#handleBareFuzzy(String, Token, String)} accessible by sub-classes.
 * It is needed for {@link QueryParser}s that need to override the parsing of the slop in a fuzzy query (e.g. word<b>~2</b>, word<b>~</b>).
 *
 * TODO: We should maybe rewrite this with the flexible query parser which matches the same syntax with more freedom.
 */
public class XQueryParser extends QueryParser {
    public XQueryParser(String f, Analyzer a) {
        super(f, a);
    }

    @Override
    protected Query handleBareFuzzy(String field, Token fuzzySlop, String termImage) throws ParseException {
        return super.handleBareFuzzy(field, fuzzySlop, termImage);
    }
}
