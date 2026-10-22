/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Rewrites {@code filter} to select documents at the nested level {@code nestedPath}. The filter is evaluated at the
     * level of the fields it reads, and a document matches when its ancestor at that level matches. Boolean and dis_max
     * filters that read several levels are rewritten clause by clause, which is exact because a nested document has one
     * ancestor per level.
     * <p>
     * The level of scripts, runtime fields, metadata fields and fields copied by {@code include_in_parent} or
     * {@code include_in_root} is unknown. A clause of unknown level is evaluated with its siblings when they read a single
     * level. Otherwise it is joined from {@code unknownLevelParentPath} when {@link #mightMatchNonNestedDocs} reports that
     * it might match documents outside {@code nestedPath}.
     *
     * @param nestedPath             the nested level of the documents to select
     * @param unknownLevelParentPath the level filters of unknown level are joined from, {@code null} for the root
     */
    public static Query joinToNestedLevel(
        Query filter,
        String nestedPath,
        @Nullable String unknownLevelParentPath,
        SearchExecutionContext context
    ) {
        Set<String> levels = nestedLevels(filter, context);
        if (levels.size() == 1) {
            // a single level: the documents to select, or an ancestor to join down from
            String level = levels.iterator().next();
            if (nestedPath.equals(level)) {
                return filter;
            }
            boolean isAncestor = level == null || nestedPath.startsWith(level + ".");
            if (isAncestor) {
                return joinDownFrom(filter, level, context);
            }
        } else if (levels.size() > 1) {
            // several levels: rewrite each clause on its own
            if (filter instanceof BooleanQuery bq) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
                for (BooleanClause clause : bq.clauses()) {
                    builder.add(joinToNestedLevel(clause.query(), nestedPath, unknownLevelParentPath, context), clause.occur());
                }
                return builder.build();
            }
            if (filter instanceof DisjunctionMaxQuery dmq) {
                List<Query> disjuncts = dmq.getDisjuncts()
                    .stream()
                    .map(q -> joinToNestedLevel(q, nestedPath, unknownLevelParentPath, context))
                    .toList();
                return new DisjunctionMaxQuery(disjuncts, dmq.getTieBreakerMultiplier());
            }
            if (filter instanceof ConstantScoreQuery csq) {
                return new ConstantScoreQuery(joinToNestedLevel(csq.getQuery(), nestedPath, unknownLevelParentPath, context));
            }
            if (filter instanceof BoostQuery bq) {
                return new BoostQuery(joinToNestedLevel(bq.getQuery(), nestedPath, unknownLevelParentPath, context), bq.getBoost());
            }
        }
        // unknown level, a level below or beside nestedPath, or several levels in a query that cannot be split
        return mightMatchNonNestedDocs(filter, nestedPath, context) ? joinDownFrom(filter, unknownLevelParentPath, context) : filter;
    }

    /**
     * Returns the nested levels of the fields that {@code query} reads, {@code null} standing for the root. A nested query
     * contributes the level it returns rather than the fields of its inner query. Fields of unknown level are skipped.
     */
    static Set<String> nestedLevels(Query query, SearchExecutionContext context) {
        Set<String> levels = new HashSet<>();
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                addFieldLevel(field, levels, context);
                return true;
            }

            @Override
            public void consumeTerms(Query query, Term... terms) {
                for (Term term : terms) {
                    // exists queries on fields without doc values or norms are terms of the _field_names field
                    if (FieldNamesFieldMapper.NAME.equals(term.field())) {
                        addFieldLevel(term.text(), levels, context);
                    }
                }
            }

            @Override
            public QueryVisitor getSubVisitor(Occur occur, Query parent) {
                if (parent instanceof ESToParentBlockJoinQuery join) {
                    levels.add(join.getPath());
                    return EMPTY_VISITOR;
                }
                return this;
            }
        });
        return levels;
    }

    private static void addFieldLevel(String field, Set<String> levels, SearchExecutionContext context) {
        if (context.isMetadataField(field) || context.isFieldMapped(field) == false) {
            return;
        }
        if (context.getFieldType(field) instanceof AbstractScriptFieldType<?>) {
            // runtime fields are computed on whichever document the script runs on
            return;
        }
        NestedLookup nestedLookup = context.nestedLookup();
        String level = nestedLookup.getNestedParent(field);
        for (String path = level; path != null; path = nestedLookup.getNestedParent(path)) {
            NestedObjectMapper mapper = nestedLookup.getNestedMappers().get(path);
            if (mapper.isIncludeInParent() || mapper.isIncludeInRoot()) {
                // the field is also indexed at an ancestor level
                return;
            }
        }
        levels.add(level);
    }

    /** Returns the documents below {@code level} whose ancestor at {@code level} matches {@code filter}. */
    private static Query joinDownFrom(Query filter, @Nullable String level, SearchExecutionContext context) {
        Query levelFilter = level == null
            ? Queries.newNonNestedFilter(context.indexVersionCreated())
            : context.nestedLookup().getNestedMappers().get(level).nestedTypeFilter();
        return new ToChildBlockJoinQuery(Queries.filtered(filter, levelFilter), context.bitsetFilter(levelFilter));
    }
}
