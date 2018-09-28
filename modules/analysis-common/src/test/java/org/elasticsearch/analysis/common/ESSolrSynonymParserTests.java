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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;

import static org.hamcrest.Matchers.containsString;

public class ESSolrSynonymParserTests extends ESTokenStreamTestCase {

    public void testLenientParser() throws IOException, ParseException {
        ESSolrSynonymParser parser = new ESSolrSynonymParser(true, false, true, new StandardAnalyzer());
        String rules =
            "&,and\n" +
            "come,advance,approach\n";
        StringReader rulesReader = new StringReader(rules);
        parser.parse(rulesReader);
        SynonymMap synonymMap = parser.build();
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader("approach quietly then advance & destroy"));
        TokenStream ts = new SynonymFilter(tokenizer, synonymMap, false);
        assertTokenStreamContents(ts, new String[]{"come", "quietly", "then", "come", "destroy"});
    }

    public void testLenientParserWithSomeIncorrectLines() throws IOException, ParseException {
        CharArraySet stopSet = new CharArraySet(1, true);
        stopSet.add("bar");
        ESSolrSynonymParser parser =
            new ESSolrSynonymParser(true, false, true, new StandardAnalyzer(stopSet));
        String rules = "foo,bar,baz";
        StringReader rulesReader = new StringReader(rules);
        parser.parse(rulesReader);
        SynonymMap synonymMap = parser.build();
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader("first word is foo, then bar and lastly baz"));
        TokenStream ts = new SynonymFilter(new StopFilter(tokenizer, stopSet), synonymMap, false);
        assertTokenStreamContents(ts, new String[]{"first", "word", "is", "foo", "then", "and", "lastly", "foo"});
    }

    public void testNonLenientParser() {
        ESSolrSynonymParser parser = new ESSolrSynonymParser(true, false, false, new StandardAnalyzer());
        String rules =
            "&,and=>and\n" +
            "come,advance,approach\n";
        StringReader rulesReader = new StringReader(rules);
        ParseException ex = expectThrows(ParseException.class, () -> parser.parse(rulesReader));
        assertThat(ex.getMessage(), containsString("Invalid synonym rule at line 1"));
    }
}
