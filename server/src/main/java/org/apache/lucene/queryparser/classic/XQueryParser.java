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
