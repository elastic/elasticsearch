/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

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
import org.elasticsearch.index.query.SearchExecutionContext;

/** Utility class to filter parent and children clauses when building nested
 * queries. */
public final class NestedHelper {

    private NestedHelper() {}

    /** Returns true if the given query might match nested documents. */
    public static boolean mightMatchNestedDocs(Query query, SearchExecutionContext searchExecutionContext) {
        if (query instanceof ConstantScoreQuery) {
            return mightMatchNestedDocs(((ConstantScoreQuery) query).getQuery(), searchExecutionContext);
        } else if (query instanceof BoostQuery) {
            return mightMatchNestedDocs(((BoostQuery) query).getQuery(), searchExecutionContext);
        } else if (query instanceof MatchAllDocsQuery) {
            return true;
        } else if (query instanceof MatchNoDocsQuery) {
            return false;
        } else if (query instanceof TermQuery) {
            // We only handle term(s) queries and range queries, which should already
            // cover a high majority of use-cases
            return mightMatchNestedDocs(((TermQuery) query).getTerm().field(), searchExecutionContext);
        } else if (query instanceof TermInSetQuery tis) {
            if (tis.getTermsCount() > 0) {
                return mightMatchNestedDocs(tis.getField(), searchExecutionContext);
            } else {
                return false;
            }
        } else if (query instanceof PointRangeQuery) {
            return mightMatchNestedDocs(((PointRangeQuery) query).getField(), searchExecutionContext);
        } else if (query instanceof IndexOrDocValuesQuery) {
            return mightMatchNestedDocs(((IndexOrDocValuesQuery) query).getIndexQuery(), searchExecutionContext);
        } else if (query instanceof final BooleanQuery bq) {
            final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
            if (hasRequiredClauses) {
                return bq.clauses()
                    .stream()
                    .filter(BooleanClause::isRequired)
                    .map(BooleanClause::query)
                    .allMatch(f -> mightMatchNestedDocs(f, searchExecutionContext));
            } else {
                return bq.clauses()
                    .stream()
                    .filter(c -> c.occur() == Occur.SHOULD)
                    .map(BooleanClause::query)
                    .anyMatch(f -> mightMatchNestedDocs(f, searchExecutionContext));
            }
        } else if (query instanceof ESToParentBlockJoinQuery) {
            return ((ESToParentBlockJoinQuery) query).getPath() != null;
        } else {
            return true;
        }
    }

    /** Returns true if a query on the given field might match nested documents. */
    private static boolean mightMatchNestedDocs(String field, SearchExecutionContext searchExecutionContext) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (searchExecutionContext.isFieldMapped(field) == false) {
            // field does not exist
            return false;
        }
        return searchExecutionContext.nestedLookup().getNestedParent(field) != null;
    }

    /** Returns true if the given query might match parent documents or documents
     *  that are nested under a different path. */
    public static boolean mightMatchNonNestedDocs(Query query, String nestedPath, SearchExecutionContext searchExecutionContext) {
        if (query instanceof ConstantScoreQuery) {
            return mightMatchNonNestedDocs(((ConstantScoreQuery) query).getQuery(), nestedPath, searchExecutionContext);
        } else if (query instanceof BoostQuery) {
            return mightMatchNonNestedDocs(((BoostQuery) query).getQuery(), nestedPath, searchExecutionContext);
        } else if (query instanceof MatchAllDocsQuery) {
            return true;
        } else if (query instanceof MatchNoDocsQuery) {
            return false;
        } else if (query instanceof TermQuery) {
            return mightMatchNonNestedDocs(searchExecutionContext, ((TermQuery) query).getTerm().field(), nestedPath);
        } else if (query instanceof TermInSetQuery tis) {
            if (tis.getTermsCount() > 0) {
                return mightMatchNonNestedDocs(searchExecutionContext, tis.getField(), nestedPath);
            } else {
                return false;
            }
        } else if (query instanceof PointRangeQuery) {
            return mightMatchNonNestedDocs(searchExecutionContext, ((PointRangeQuery) query).getField(), nestedPath);
        } else if (query instanceof IndexOrDocValuesQuery) {
            return mightMatchNonNestedDocs(((IndexOrDocValuesQuery) query).getIndexQuery(), nestedPath, searchExecutionContext);
        } else if (query instanceof final BooleanQuery bq) {
            final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
            if (hasRequiredClauses) {
                return bq.clauses()
                    .stream()
                    .filter(BooleanClause::isRequired)
                    .map(BooleanClause::query)
                    .allMatch(q -> mightMatchNonNestedDocs(q, nestedPath, searchExecutionContext));
            } else {
                return bq.clauses()
                    .stream()
                    .filter(c -> c.occur() == Occur.SHOULD)
                    .map(BooleanClause::query)
                    .anyMatch(q -> mightMatchNonNestedDocs(q, nestedPath, searchExecutionContext));
            }
        } else {
            return true;
        }
    }

    /** Returns true if a query on the given field might match parent documents
     *  or documents that are nested under a different path. */
    private static boolean mightMatchNonNestedDocs(SearchExecutionContext searchExecutionContext, String field, String nestedPath) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (searchExecutionContext.isFieldMapped(field) == false) {
            return false;
        }
        var nestedLookup = searchExecutionContext.nestedLookup();
        String nestedParent = nestedLookup.getNestedParent(field);
        if (nestedParent == null || nestedParent.startsWith(nestedPath) == false) {
            // the field is not a sub field of the nested path
            return true;
        }
        NestedObjectMapper nestedMapper = nestedLookup.getNestedMappers().get(nestedParent);
        // If the mapper does not include in its parent or in the root object then
        // the query might only match nested documents with the given path
        if (nestedParent.equals(nestedPath)) {
            return nestedMapper.isIncludeInParent() || nestedMapper.isIncludeInRoot();
        }
        return true;
    }
}
