/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.containsString;

public class ESWordnetSynonymParserTests extends ESTokenStreamTestCase {

    public void testLenientParser() throws IOException, ParseException {
        ESWordnetSynonymParser parser = new ESWordnetSynonymParser(true, false, true, new StandardAnalyzer());
        String rules = """
            s(100000001,1,'&',a,1,0).
            s(100000001,2,'and',a,1,0).
            s(100000002,1,'come',v,1,0).
            s(100000002,2,'advance',v,1,0).
            s(100000002,3,'approach',v,1,0).""";
        StringReader rulesReader = new StringReader(rules);
        parser.parse(rulesReader);
        SynonymMap synonymMap = parser.build();
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader("approach quietly then advance & destroy"));
        TokenStream ts = new SynonymFilter(tokenizer, synonymMap, false);
        assertTokenStreamContents(ts, new String[] { "come", "quietly", "then", "come", "destroy" });
    }

    public void testLenientParserWithSomeIncorrectLines() throws IOException, ParseException {
        CharArraySet stopSet = new CharArraySet(1, true);
        stopSet.add("bar");
        ESWordnetSynonymParser parser = new ESWordnetSynonymParser(true, false, true, new StandardAnalyzer(stopSet));
        String rules = """
            s(100000001,1,'foo',v,1,0).
            s(100000001,2,'bar',v,1,0).
            s(100000001,3,'baz',v,1,0).""";
        StringReader rulesReader = new StringReader(rules);
        parser.parse(rulesReader);
        SynonymMap synonymMap = parser.build();
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader("first word is foo, then bar and lastly baz"));
        TokenStream ts = new SynonymFilter(new StopFilter(tokenizer, stopSet), synonymMap, false);
        assertTokenStreamContents(ts, new String[] { "first", "word", "is", "foo", "then", "and", "lastly", "foo" });
    }

    public void testNonLenientParser() {
        ESWordnetSynonymParser parser = new ESWordnetSynonymParser(true, false, false, new StandardAnalyzer());
        String rules = """
            s(100000001,1,'&',a,1,0).
            s(100000001,2,'and',a,1,0).
            s(100000002,1,'come',v,1,0).
            s(100000002,2,'advance',v,1,0).
            s(100000002,3,'approach',v,1,0).""";
        StringReader rulesReader = new StringReader(rules);
        ParseException ex = expectThrows(ParseException.class, () -> parser.parse(rulesReader));
        assertThat(ex.getMessage(), containsString("Invalid synonym rule at line 1"));
    }

}
