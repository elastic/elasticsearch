/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.EmptyField;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Provide access to DocValues for script {@code field} api and {@code doc} API.
 */
public class DocValuesDocReader implements DocReader, LeafReaderContextSupplier {

    protected final SearchLookup searchLookup;

    // provide access to the leaf context reader for expressions
    protected final LeafReaderContext leafReaderContext;
    private final TermStatsReader termStatsReader;

    /** A leaf lookup for the bound segment this proxy will operate on. */
    protected LeafSearchLookup leafSearchLookup;

    public DocValuesDocReader(SearchLookup searchLookup, LeafReaderContext leafContext) {
        this(searchLookup, leafContext, null);
    }

    public DocValuesDocReader(SearchLookup searchLookup, LeafReaderContext leafContext, TermStatsReader termStatsReader) {
        this.searchLookup = searchLookup;
        this.leafReaderContext = leafContext;
        this.leafSearchLookup = searchLookup.getLeafSearchLookup(leafReaderContext);
        this.termStatsReader = termStatsReader;
    }

    @Override
    public Field<?> field(String fieldName) {
        LeafDocLookup leafDocLookup = leafSearchLookup.doc();

        if (leafDocLookup.containsKey(fieldName) == false) {
            return new EmptyField(fieldName);
        }

        return leafDocLookup.getScriptField(fieldName);
    }

    @Override
    public Stream<Field<?>> fields(String fieldGlob) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setDocument(int docID) {
        leafSearchLookup.setDocument(docID);
    }

    @Override
    public Map<String, Object> docAsMap() {
        return leafSearchLookup.asMap();
    }

    @Override
    public Map<String, ScriptDocValues<?>> doc() {
        return leafSearchLookup.doc();
    }

    @Override
    public LeafReaderContext getLeafReaderContext() {
        return leafReaderContext;
    }

    @Override
    public Supplier<Source> source() {
        return leafSearchLookup.source();
    }

    @Override
    public Map<Term, TermStatistics> termStatistics() {
        Map<Term, TermStatistics> termStatistics = new HashMap<>();
        for (Term term: termStatsReader.terms()) {
            termStatistics.put(term, termStatsReader.termStatistics(term));
        }
        return termStatistics;
    }

    @Override
    public Map<Term, PostingsEnum> postings(int flags) {
        Map<Term, PostingsEnum> postings = new HashMap<>();
        for (Term term: termStatsReader.terms()) {
            postings.put(term, termStatsReader.postings(leafReaderContext, term, flags));
        }
        return postings;
    }
}
