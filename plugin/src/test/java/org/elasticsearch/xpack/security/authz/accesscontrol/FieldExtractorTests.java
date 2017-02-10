/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AssertingQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.DocValuesNumbersQuery;
import org.apache.lucene.search.DocValuesRangeQuery;
import org.apache.lucene.search.FieldValueQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Simple tests for query field extraction */
public class FieldExtractorTests extends ESTestCase {

    public void testBoolean() {
        Set<String> fields = new HashSet<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("no", "baz")), BooleanClause.Occur.MUST_NOT);
        FieldExtractor.extractFields(builder.build(), fields);
        assertEquals(asSet("foo", "no"), fields);
    }
    
    public void testDisjunctionMax() {
        Set<String> fields = new HashSet<>();
        DisjunctionMaxQuery query = new DisjunctionMaxQuery(Arrays.asList(
          new TermQuery(new Term("one", "bar")),
          new TermQuery(new Term("two", "baz"))
        ), 1.0F);
        FieldExtractor.extractFields(query, fields);
        assertEquals(asSet("one", "two"), fields);
    }
    
    public void testSpanTerm() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new SpanTermQuery(new Term("foo", "bar")), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testTerm() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new TermQuery(new Term("foo", "bar")), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testSynonym() {
        Set<String> fields = new HashSet<>();
        SynonymQuery query = new SynonymQuery(new Term("foo", "bar"), new Term("foo", "baz"));
        FieldExtractor.extractFields(query, fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testPhrase() {
        Set<String> fields = new HashSet<>();
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.add(new Term("foo", "bar"));
        builder.add(new Term("foo", "baz"));
        FieldExtractor.extractFields(builder.build(), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testMultiPhrase() {
        Set<String> fields = new HashSet<>();
        MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder();
        builder.add(new Term("foo", "bar"));
        builder.add(new Term[] { new Term("foo", "baz"), new Term("foo", "baz2") });
        FieldExtractor.extractFields(builder.build(), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testPointRange() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(IntPoint.newRangeQuery("foo", 3, 4), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testPointSet() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(IntPoint.newSetQuery("foo", 3, 4, 5), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testFieldValue() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new FieldValueQuery("foo"), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testDocValuesNumbers() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new DocValuesNumbersQuery("foo", 5L), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testDocValuesRange() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(DocValuesRangeQuery.newLongRange("foo", 1L, 2L, true, true), fields);
        assertEquals(asSet("foo"), fields);
    }
    
    public void testMatchAllDocs() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new MatchAllDocsQuery(), fields);
        assertEquals(Collections.emptySet(), fields);
    }
    
    public void testMatchNoDocs() {
        Set<String> fields = new HashSet<>();
        FieldExtractor.extractFields(new MatchNoDocsQuery(), fields);
        assertEquals(Collections.emptySet(), fields);
    }
    
    public void testUnsupported() {
        Set<String> fields = new HashSet<>();
        expectThrows(UnsupportedOperationException.class, () -> {
            FieldExtractor.extractFields(new AssertingQuery(random(), new MatchAllDocsQuery()), fields);
        });
    }
}
