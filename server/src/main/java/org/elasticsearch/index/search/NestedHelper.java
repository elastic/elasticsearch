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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.List;

/** Utility class to filter parent and children clauses when building nested
 * queries. */
public final class NestedHelper {

    private NestedHelper() {}

    /**
     * Returns true if the given query might match nested documents.
     *
     * <p>Note: {@link Occur#MUST_NOT} clauses are intentionally not inspected because they can only
     * narrow the set of matching documents, never widen it. The positive clauses (MUST, FILTER, SHOULD)
     * determine the maximum possible match set, and MUST_NOT exclusions can only reduce it. Therefore,
     * if the positive clauses might match nested docs, the overall query might too, regardless of what
     * MUST_NOT excludes.
     */
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

    /**
     * Returns true if the given query might match parent documents or documents
     * that are nested under a different path.
     *
     * <p>Note: {@link Occur#MUST_NOT} clauses are intentionally not inspected because they can only
     * narrow the set of matching documents, never widen it. However, this means that a pure-negative
     * query like {@code +MatchAllDocsQuery -nested.field:value} will return {@code true} (driven by
     * the synthetic {@link MatchAllDocsQuery}), even though the intent is to filter nested documents.
     * Callers that need to distinguish parent-level from child-level clauses in such mixed queries
     * should use {@link #decomposeFilter} instead.
     */
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

    /**
     * Result of decomposing a kNN filter query into parent-level and child-level clauses.
     * Used exclusively during kNN vector search filter processing to correctly handle
     * filters containing {@code must_not} clauses on nested fields.
     *
     * @param parentClauses clauses that might match non-nested (parent) documents
     * @param childClauses clauses that only match nested documents at the target path
     */
    public record DecomposedFilter(List<BooleanClause> parentClauses, List<BooleanClause> childClauses) {
        /** Returns true if both parent and child clauses are present, indicating the filter needs splitting. */
        public boolean hasBothLevels() {
            return parentClauses.isEmpty() == false && childClauses.isEmpty() == false;
        }

        /** Returns true if all clauses are child-level (no parent clauses), e.g. a pure must_not on nested fields. */
        public boolean isChildOnly() {
            return parentClauses.isEmpty() && childClauses.isEmpty() == false;
        }
    }

    /**
     * Decomposes a kNN filter query's boolean clauses into those that target parent/non-nested
     * documents and those that target only nested documents at the given path. This is used
     * exclusively during kNN vector search filter processing to correctly handle filters
     * containing {@code must_not} clauses on nested fields, which would otherwise become
     * no-ops when the entire filter is wrapped with {@code ToChildBlockJoinQuery}.
     *
     * <p>For example, a query like {@code bool { must: [term parent_field], must_not: [term nested_field] }}
     * would be decomposed so the {@code must} clause is classified as parent-level (to be wrapped with
     * {@code ToChildBlockJoinQuery}) while the {@code must_not} clause is classified as child-level
     * (to be applied directly against nested documents).
     *
     * <p>The decomposition is recursive: if a MUST or FILTER clause is itself a {@link BooleanQuery}
     * containing a mix of parent-level and child-level sub-clauses, those sub-clauses are recursively
     * decomposed and merged into the outer parent/child lists. This handles arbitrarily nested
     * conjunctive boolean trees (e.g., {@code bool { must: [bool { must: [term parent], must_not: [term nested] }] }}).
     *
     * <p>Recursion is <strong>not attempted</strong> (returns {@code null}) when the boolean structure
     * would make decomposition semantically incorrect:
     * <ul>
     *   <li>Any SHOULD clauses — OR semantics cannot be preserved when splitting clauses across
     *       parent/child levels, as decomposing would turn OR into AND</li>
     *   <li>MUST_NOT clauses wrapping a mixed-level sub-BooleanQuery — negation of a conjunction
     *       produces a disjunction (De Morgan's law), which cannot be split</li>
     * </ul>
     * In these cases, the caller falls back to wrapping the entire filter monolithically, which is
     * safe but may cause nested {@code must_not} clauses to be silently ignored (the pre-existing behavior).
     *
     * <p>A synthetic {@link MatchAllDocsQuery} added by
     * {@link org.elasticsearch.common.lucene.search.Queries#fixNegativeQueryIfNeeded} to pure-negative
     * queries is detected and kept with the child-level {@code MUST_NOT} clauses rather than being
     * classified as a parent-level clause.
     *
     * @param query the filter query to decompose
     * @param nestedPath the nested path that the vector field belongs to
     * @param searchExecutionContext the search execution context
     * @return a {@link DecomposedFilter} if the query is a {@link BooleanQuery} whose clauses
     *         need special handling (mixed parent/child, or pure child-level with synthetic
     *         MatchAllDocsQuery from a negation-only filter), or {@code null} if the query
     *         cannot be decomposed or needs no special treatment
     */
    @Nullable
    public static DecomposedFilter decomposeFilter(Query query, String nestedPath, SearchExecutionContext searchExecutionContext) {
        Query unwrapped = query;
        if (unwrapped instanceof ConstantScoreQuery csq) {
            unwrapped = csq.getQuery();
        } else if (unwrapped instanceof BoostQuery bq) {
            unwrapped = bq.getQuery();
        }

        if (unwrapped instanceof BooleanQuery == false) {
            return null;
        }
        BooleanQuery bq = (BooleanQuery) unwrapped;

        List<BooleanClause> parentClauses = new ArrayList<>();
        List<BooleanClause> childClauses = new ArrayList<>();
        // Track MatchAllDocsQuery FILTER clauses separately — these may be synthetic
        // (added by fixNegativeQueryIfNeeded) and should stay with child MUST_NOT clauses
        List<BooleanClause> matchAllFilterClauses = new ArrayList<>();

        if (decomposeFilterClauses(bq, nestedPath, searchExecutionContext, parentClauses, childClauses, matchAllFilterClauses) == false) {
            return null;
        }

        // Assign synthetic MatchAllDocsQuery clauses: if there are child-level MUST_NOT clauses
        // and no other child-level required clauses, the MatchAllDocsQuery is the synthetic one
        // from fixNegativeQueryIfNeeded and belongs with the child clauses
        boolean hasChildMustNot = childClauses.stream().anyMatch(c -> c.occur() == Occur.MUST_NOT);
        boolean hasChildRequired = childClauses.stream().anyMatch(BooleanClause::isRequired);
        if (hasChildMustNot && hasChildRequired == false && matchAllFilterClauses.isEmpty() == false) {
            childClauses.addAll(matchAllFilterClauses);
        } else {
            parentClauses.addAll(matchAllFilterClauses);
        }

        // If child clauses contain only MUST_NOT with no positive clause (e.g., from a mixed bool
        // where the positive clauses were all parent-level), add a synthetic MatchAllDocsQuery so
        // the resulting BooleanQuery is not pure-negative (which would match nothing in Lucene).
        if (hasChildMustNot && childClauses.stream().noneMatch(BooleanClause::isRequired)) {
            childClauses.add(new BooleanClause(Queries.ALL_DOCS_INSTANCE, Occur.FILTER));
        }

        if (childClauses.isEmpty()) {
            return null;
        }
        if (parentClauses.isEmpty()) {
            // Only return decomposed for pure must_not on nested fields with synthetic MatchAll,
            // so the caller knows NOT to wrap with ToChildBlockJoinQuery
            return (hasChildMustNot && matchAllFilterClauses.isEmpty() == false) ? new DecomposedFilter(parentClauses, childClauses) : null;
        }

        return new DecomposedFilter(parentClauses, childClauses);
    }

    /**
     * Recursively classifies the clauses of a {@link BooleanQuery} into parent-level and child-level lists.
     * Returns {@code true} if decomposition succeeded, {@code false} if the structure cannot be safely
     * decomposed (e.g., SHOULD with mixed levels, MUST_NOT wrapping mixed sub-booleans).
     *
     * <p>The {@code matchAllFilterClauses} list intentionally accumulates across recursion levels.
     * Synthetic {@link MatchAllDocsQuery} clauses from any level are collected into this shared list
     * and assigned to the appropriate group (parent or child) by the caller ({@link #decomposeFilter})
     * after all recursion completes. This is safe because the assignment logic only depends on whether
     * child-level MUST_NOT clauses exist, not on which recursion level produced the MatchAllDocsQuery.
     */
    private static boolean decomposeFilterClauses(
        BooleanQuery bq,
        String nestedPath,
        SearchExecutionContext searchExecutionContext,
        List<BooleanClause> parentClauses,
        List<BooleanClause> childClauses,
        List<BooleanClause> matchAllFilterClauses
    ) {
        for (BooleanClause clause : bq) {
            if (clause.isRequired() && clause.query() instanceof MatchAllDocsQuery) {
                matchAllFilterClauses.add(clause);
                continue;
            }

            Query clauseQuery = clause.query();
            // Unwrap ConstantScoreQuery/BoostQuery so we can detect and recurse into sub-BooleanQueries
            if (clauseQuery instanceof ConstantScoreQuery csq) {
                clauseQuery = csq.getQuery();
            } else if (clauseQuery instanceof BoostQuery boostQ) {
                clauseQuery = boostQ.getQuery();
            }

            if (clause.occur() == Occur.SHOULD) {
                // SHOULD semantics (OR) cannot be preserved when splitting clauses across parent/child
                // levels — decomposing would turn OR into AND. Bail out and let the caller fall back
                // to monolithic wrapping which preserves SHOULD semantics.
                return false;
            }

            boolean matchesNonNested = mightMatchNonNestedDocs(clause.query(), nestedPath, searchExecutionContext);

            if (matchesNonNested
                && clauseQuery instanceof BooleanQuery subBool
                && hasAnyNestedClause(subBool, nestedPath, searchExecutionContext)) {
                // The sub-boolean has clauses targeting both levels (including in MUST_NOT).
                // mightMatchNestedDocs ignores MUST_NOT, so we use hasAnyNestedClause to detect
                // nested targeting in any clause type.
                if (clause.isRequired()) {
                    // MUST/FILTER with a mixed sub-boolean: recurse into it
                    if (decomposeFilterClauses(
                        subBool,
                        nestedPath,
                        searchExecutionContext,
                        parentClauses,
                        childClauses,
                        matchAllFilterClauses
                    ) == false) {
                        return false;
                    }
                } else {
                    // MUST_NOT wrapping a mixed sub-boolean: cannot decompose safely
                    // (negation of a conjunction produces a disjunction via De Morgan's law)
                    return false;
                }
            } else if (matchesNonNested) {
                parentClauses.add(clause);
            } else {
                childClauses.add(clause);
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if any clause in the given {@link BooleanQuery} (including {@link Occur#MUST_NOT}
     * clauses) might target nested documents at the given path. This differs from {@link #mightMatchNestedDocs}
     * which intentionally ignores MUST_NOT clauses. Used by recursive decomposition to detect sub-booleans
     * that contain nested field references in any position.
     *
     * <p>This check is conservative: it uses {@code mightMatchNonNestedDocs(clause) == false} as a proxy
     * for "targets nested docs." Unrecognized query types default to {@code mightMatchNonNestedDocs == true},
     * so nested clauses wrapped in unrecognized query types may be missed. In that case, the sub-boolean
     * is not identified as mixed, recursion is skipped, and the caller falls back to monolithic wrapping —
     * which is safe but may not correctly handle nested {@code must_not} clauses.
     */
    private static boolean hasAnyNestedClause(BooleanQuery bq, String nestedPath, SearchExecutionContext searchExecutionContext) {
        for (BooleanClause clause : bq) {
            if (mightMatchNonNestedDocs(clause.query(), nestedPath, searchExecutionContext) == false) {
                // This clause targets only nested docs at nestedPath
                return true;
            }
            Query clauseQuery = clause.query();
            // Unwrap ConstantScoreQuery/BoostQuery so we can detect sub-BooleanQueries
            if (clauseQuery instanceof ConstantScoreQuery csq) {
                clauseQuery = csq.getQuery();
            } else if (clauseQuery instanceof BoostQuery boostQ) {
                clauseQuery = boostQ.getQuery();
            }
            if (clauseQuery instanceof BooleanQuery subBool && hasAnyNestedClause(subBool, nestedPath, searchExecutionContext)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds a {@link BooleanQuery} from the given list of clauses.
     */
    public static Query toBooleanQuery(List<BooleanClause> clauses) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (BooleanClause clause : clauses) {
            builder.add(clause);
        }
        return builder.build();
    }
}
