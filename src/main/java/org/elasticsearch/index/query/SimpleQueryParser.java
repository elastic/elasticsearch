/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.XSimpleQueryParser;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

/**
 * Wrapper class for Lucene's SimpleQueryParser that allows us to redefine
 * different types of queries.
 */
public class SimpleQueryParser extends XSimpleQueryParser {

    private final boolean lowercaseExpandedTerms;

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, boolean lowercaseExpandedTerms) {
        super(analyzer, weights, flags);
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newFuzzyQuery, optionally
     * lowercasing the term first
     */
    @Override
    public Query newFuzzyQuery(String text, int fuzziness) {
        if (lowercaseExpandedTerms) {
            text = text.toLowerCase(Locale.ROOT);
        }
        return super.newFuzzyQuery(text, fuzziness);
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newPrefixQuery, optionally
     * lowercasing the term first
     */
    @Override
    public Query newPrefixQuery(String text) {
        if (lowercaseExpandedTerms) {
            text = text.toLowerCase(Locale.ROOT);
        }
        return super.newPrefixQuery(text);
    }
}
