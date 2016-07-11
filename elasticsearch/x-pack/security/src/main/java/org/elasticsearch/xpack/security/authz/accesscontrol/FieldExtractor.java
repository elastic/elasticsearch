/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

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
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.spans.SpanTermQuery;

import java.util.Set;

/** 
 * Extracts fields from a query, or throws UnsupportedOperationException.
 * <p>
 * Lucene queries have {@link Weight#extractTerms}, but this is really geared at things
 * such as highlighting, not security. For example terms in a Boolean {@code MUST_NOT} clause
 * are not included, TermsQuery doesn't implement the method as it could be terribly slow, etc.
 */
class FieldExtractor {

    /**
     * Populates {@code fields} with the set of fields used by the query, or throws 
     * UnsupportedOperationException if it doesn't know how to do this.
     */
    static void extractFields(Query query, Set<String> fields) throws UnsupportedOperationException {
        // NOTE: we expect a rewritten query, so we only need logic for "atomic" queries here:
        if (query instanceof BooleanQuery) {
            // extract from all clauses
            BooleanQuery q = (BooleanQuery) query;
            for (BooleanClause clause : q.clauses()) {
                extractFields(clause.getQuery(), fields);
            }
        } else if (query instanceof DisjunctionMaxQuery) {
            // extract from all clauses
            DisjunctionMaxQuery q = (DisjunctionMaxQuery) query;
            for (Query clause : q.getDisjuncts()) {
                extractFields(clause, fields);
            }
        } else if (query instanceof SpanTermQuery) {
            // we just do SpanTerm, other spans are trickier, they could contain 
            // the evil FieldMaskingSpanQuery: so SpanQuery.getField cannot be trusted.
            fields.add(((SpanTermQuery)query).getField());
        } else if (query instanceof TermQuery) {
            fields.add(((TermQuery)query).getTerm().field());
        } else if (query instanceof SynonymQuery) {
            SynonymQuery q = (SynonymQuery) query;
            // all terms must have the same field
            fields.add(q.getTerms().get(0).field());
        } else if (query instanceof PhraseQuery) {
            PhraseQuery q = (PhraseQuery) query;
            // all terms must have the same field
            fields.add(q.getTerms()[0].field());
        } else if (query instanceof MultiPhraseQuery) {
            MultiPhraseQuery q = (MultiPhraseQuery) query;
            // all terms must have the same field
            fields.add(q.getTermArrays()[0][0].field());
        } else if (query instanceof PointRangeQuery) {
            fields.add(((PointRangeQuery)query).getField());
        } else if (query instanceof PointInSetQuery) {
            fields.add(((PointInSetQuery)query).getField());
        } else if (query instanceof FieldValueQuery) {
            fields.add(((FieldValueQuery)query).getField());
        } else if (query instanceof DocValuesNumbersQuery) {
            fields.add(((DocValuesNumbersQuery)query).getField());
        } else if (query instanceof DocValuesRangeQuery) {
            fields.add(((DocValuesRangeQuery)query).getField());
        } else if (query instanceof MatchAllDocsQuery) {
            // no field
        } else if (query instanceof MatchNoDocsQuery) {
            // no field
        } else {
            throw new UnsupportedOperationException(); // we don't know how to get the fields from it
        }
    }
}
