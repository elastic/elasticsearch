/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.sandbox.search.DocValuesNumbersQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.util.set.Sets;

import java.util.Set;

/**
 * Extracts fields from a query, or throws UnsupportedOperationException.
 * <p>
 * Lucene queries used to have Weight#extractTerms, but this was really geared at things
 * such as highlighting, not security. For example terms in a Boolean {@code MUST_NOT} clause
 * are not included, TermsQuery doesn't implement the method as it could be terribly slow, etc.
 *
 * TODO reimplement this using QueryVisitors
 */
class FieldExtractor {

    /**
     * Populates {@code fields} with the set of fields used by the query, or throws
     * UnsupportedOperationException if it doesn't know how to do this.
     */
    static void extractFields(Query query, Set<String> fields) throws UnsupportedOperationException {
        // NOTE: we expect a rewritten query, so we only need logic for "atomic" queries here:
        if (query instanceof BooleanQuery q) {
            // extract from all clauses
            for (BooleanClause clause : q.clauses()) {
                extractFields(clause.getQuery(), fields);
            }
        } else if (query instanceof DisjunctionMaxQuery q) {
            // extract from all clauses
            for (Query clause : q.getDisjuncts()) {
                extractFields(clause, fields);
            }
        } else if (query instanceof SpanTermQuery spanTermQuery) {
            // we just do SpanTerm, other spans are trickier, they could contain
            // the evil FieldMaskingSpanQuery: so SpanQuery.getField cannot be trusted.
            fields.add(spanTermQuery.getField());
        } else if (query instanceof TermQuery termQuery) {
            fields.add(termQuery.getTerm().field());
        } else if (query instanceof SynonymQuery q) {
            // all terms must have the same field
            fields.add(q.getTerms().get(0).field());
        } else if (query instanceof PhraseQuery q) {
            // all terms must have the same field
            fields.add(q.getTerms()[0].field());
        } else if (query instanceof MultiPhraseQuery q) {
            // all terms must have the same field
            fields.add(q.getTermArrays()[0][0].field());
        } else if (query instanceof PointRangeQuery pointRangeQuery) {
            fields.add(pointRangeQuery.getField());
        } else if (query instanceof PointInSetQuery pointInSetQuery) {
            fields.add(pointInSetQuery.getField());
        } else if (query instanceof FieldExistsQuery fieldExistsQuery) {
            fields.add(fieldExistsQuery.getField());
        } else if (query instanceof DocValuesNumbersQuery docValuesNumbersQuery) {
            fields.add(docValuesNumbersQuery.getField());
        } else if (query instanceof IndexOrDocValuesQuery indexOrDocValuesQuery) {
            // Both queries are supposed to be equivalent, so if any of them can be extracted, we are good
            try {
                Set<String> dvQueryFields = Sets.newHashSetWithExpectedSize(1);
                extractFields(indexOrDocValuesQuery.getRandomAccessQuery(), dvQueryFields);
                fields.addAll(dvQueryFields);
            } catch (UnsupportedOperationException e) {
                extractFields(indexOrDocValuesQuery.getIndexQuery(), fields);
            }
        } else if (query instanceof TermInSetQuery termInSetQuery) {
            // TermInSetQuery#field is inaccessible
            TermIterator termIterator = termInSetQuery.getTermData().iterator();
            // there should only be one field
            if (termIterator.next() != null) {
                fields.add(termIterator.field());
            }
        } else if (query instanceof MatchAllDocsQuery) {
            // no field
        } else if (query instanceof MatchNoDocsQuery) {
            // no field
        } else {
            throw new UnsupportedOperationException(); // we don't know how to get the fields from it
        }
    }
}
