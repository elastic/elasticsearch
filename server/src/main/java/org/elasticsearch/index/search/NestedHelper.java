/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.function.Function;
import java.util.function.Predicate;

/** Utility class to filter parent and children clauses when building nested
 * queries. */
public final class NestedHelper {

    private final Function<String, ObjectMapper> objectMapperLookup;
    private final Predicate<String> isMappedFieldPredicate;

    public NestedHelper(Function<String, ObjectMapper> objectMapperLookup, Predicate<String> isMappedFieldPredicate) {
        this.objectMapperLookup = objectMapperLookup;
        this.isMappedFieldPredicate = isMappedFieldPredicate;
    }

    /** Returns true if the given query might match nested documents. */
    public boolean mightMatchNestedDocs(Query query) {
        if (query instanceof ConstantScoreQuery) {
            return mightMatchNestedDocs(((ConstantScoreQuery) query).getQuery());
        } else if (query instanceof BoostQuery) {
            return mightMatchNestedDocs(((BoostQuery) query).getQuery());
        } else if (query instanceof MatchAllDocsQuery) {
            return true;
        } else if (query instanceof MatchNoDocsQuery) {
            return false;
        } else if (query instanceof TermQuery) {
            // We only handle term(s) queries and range queries, which should already
            // cover a high majority of use-cases
            return mightMatchNestedDocs(((TermQuery) query).getTerm().field());
        }  else if (query instanceof TermInSetQuery) {
            PrefixCodedTerms terms = ((TermInSetQuery) query).getTermData();
            if (terms.size() > 0) {
                PrefixCodedTerms.TermIterator it = terms.iterator();
                it.next();
                return mightMatchNestedDocs(it.field());
            } else {
                return false;
            }
        } else if (query instanceof PointRangeQuery) {
            return mightMatchNestedDocs(((PointRangeQuery) query).getField());
        } else if (query instanceof IndexOrDocValuesQuery) {
            return mightMatchNestedDocs(((IndexOrDocValuesQuery) query).getIndexQuery());
        } else if (query instanceof BooleanQuery) {
            final BooleanQuery bq = (BooleanQuery) query;
            final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
            if (hasRequiredClauses) {
                return bq.clauses().stream()
                        .filter(BooleanClause::isRequired)
                        .map(BooleanClause::getQuery)
                        .allMatch(this::mightMatchNestedDocs);
            } else {
                return bq.clauses().stream()
                        .filter(c -> c.getOccur() == Occur.SHOULD)
                        .map(BooleanClause::getQuery)
                        .anyMatch(this::mightMatchNestedDocs);
            }
        } else if (query instanceof ESToParentBlockJoinQuery) {
            return ((ESToParentBlockJoinQuery) query).getPath() != null;
        } else {
            return true;
        }
    }

    /** Returns true if a query on the given field might match nested documents. */
    boolean mightMatchNestedDocs(String field) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (isMappedFieldPredicate.test(field) == false) {
            // field does not exist
            return false;
        }
        for (String parent = parentObject(field); parent != null; parent = parentObject(parent)) {
            ObjectMapper mapper = objectMapperLookup.apply(parent);
            if (mapper != null && mapper.isNested()) {
                return true;
            }
        }
        return false;
    }

    /** Returns true if the given query might match parent documents or documents
     *  that are nested under a different path. */
    public boolean mightMatchNonNestedDocs(Query query, String nestedPath) {
        if (query instanceof ConstantScoreQuery) {
            return mightMatchNonNestedDocs(((ConstantScoreQuery) query).getQuery(), nestedPath);
        } else if (query instanceof BoostQuery) {
            return mightMatchNonNestedDocs(((BoostQuery) query).getQuery(), nestedPath);
        } else if (query instanceof MatchAllDocsQuery) {
            return true;
        } else if (query instanceof MatchNoDocsQuery) {
            return false;
        } else if (query instanceof TermQuery) {
            return mightMatchNonNestedDocs(((TermQuery) query).getTerm().field(), nestedPath);
        } else if (query instanceof TermInSetQuery) {
            PrefixCodedTerms terms = ((TermInSetQuery) query).getTermData();
            if (terms.size() > 0) {
                PrefixCodedTerms.TermIterator it = terms.iterator();
                it.next();
                return mightMatchNonNestedDocs(it.field(), nestedPath);
            } else {
                return false;
            }
        } else if (query instanceof PointRangeQuery) {
            return mightMatchNonNestedDocs(((PointRangeQuery) query).getField(), nestedPath);
        } else if (query instanceof IndexOrDocValuesQuery) {
            return mightMatchNonNestedDocs(((IndexOrDocValuesQuery) query).getIndexQuery(), nestedPath);
        } else if (query instanceof BooleanQuery) {
            final BooleanQuery bq = (BooleanQuery) query;
            final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
            if (hasRequiredClauses) {
                return bq.clauses().stream()
                        .filter(BooleanClause::isRequired)
                        .map(BooleanClause::getQuery)
                        .allMatch(q -> mightMatchNonNestedDocs(q, nestedPath));
            } else {
                return bq.clauses().stream()
                        .filter(c -> c.getOccur() == Occur.SHOULD)
                        .map(BooleanClause::getQuery)
                        .anyMatch(q -> mightMatchNonNestedDocs(q, nestedPath));
            }
        } else {
            return true;
        }
    }

    /** Returns true if a query on the given field might match parent documents
     *  or documents that are nested under a different path. */
    boolean mightMatchNonNestedDocs(String field, String nestedPath) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (isMappedFieldPredicate.test(field) == false) {
            return false;
        }
        for (String parent = parentObject(field); parent != null; parent = parentObject(parent)) {
            ObjectMapper mapper = objectMapperLookup.apply(parent);
            if (mapper != null && mapper.isNested()) {
                NestedObjectMapper nestedMapper = (NestedObjectMapper) mapper;
                if (mapper.fullPath().equals(nestedPath)) {
                    // If the mapper does not include in its parent or in the root object then
                    // the query might only match nested documents with the given path
                    return nestedMapper.isIncludeInParent() || nestedMapper.isIncludeInRoot();
                } else {
                    // the first parent nested mapper does not have the expected path
                    // It might be misconfiguration or a sub nested mapper
                    return true;
                }
            }
        }
        return true; // the field is not a sub field of the nested path
    }

    public static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }

}
