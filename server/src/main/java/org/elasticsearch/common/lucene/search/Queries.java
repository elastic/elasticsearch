/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class Queries {

    public static Query newMatchAllQuery() {
        return new MatchAllDocsQuery();
    }

    /** Return a query that matches no document. */
    public static Query newMatchNoDocsQuery(String reason) {
        return new MatchNoDocsQuery(reason);
    }

    public static Query newUnmappedFieldQuery(String field) {
        return newUnmappedFieldsQuery(Collections.singletonList(field));
    }

    public static Query newUnmappedFieldsQuery(Collection<String> fields) {
        return Queries.newMatchNoDocsQuery("unmapped fields " + fields);
    }

    public static Query newLenientFieldQuery(String field, RuntimeException e) {
        String message = ElasticsearchException.getExceptionName(e) + ":[" + e.getMessage() + "]";
        return Queries.newMatchNoDocsQuery("failed [" + field + "] query, caused by " + message);
    }

    private static final IndexVersion NESTED_DOCS_IDENTIFIED_VIA_PRIMARY_TERMS_VERSION = IndexVersion.fromId(6010099);

    /**
     * Creates a new nested docs query
     * @param indexVersionCreated the index version created since newer indices can identify a parent field more efficiently
     */
    public static Query newNestedFilter(IndexVersion indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(NESTED_DOCS_IDENTIFIED_VIA_PRIMARY_TERMS_VERSION)) {
            return not(newNonNestedFilter(indexVersionCreated));
        } else {
            return new PrefixQuery(new Term(NestedPathFieldMapper.NAME_PRE_V8, new BytesRef("__")));
        }
    }

    /**
     * Creates a new non-nested docs query
     * @param indexVersionCreated the index version created since newer indices can identify a parent field more efficiently
     */
    public static Query newNonNestedFilter(IndexVersion indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(NESTED_DOCS_IDENTIFIED_VIA_PRIMARY_TERMS_VERSION)) {
            return new FieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        } else {
            return not(newNestedFilter(indexVersionCreated));
        }
    }

    public static BooleanQuery filtered(@Nullable Query query, @Nullable Query filter) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (query != null) {
            builder.add(new BooleanClause(query, Occur.MUST));
        }
        if (filter != null) {
            builder.add(new BooleanClause(filter, Occur.FILTER));
        }
        return builder.build();
    }

    /** Return a query that matches all documents but those that match the given query. */
    public static Query not(Query q) {
        return new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST).add(q, Occur.MUST_NOT).build();
    }

    static boolean isNegativeQuery(Query q) {
        if ((q instanceof BooleanQuery) == false) {
            return false;
        }
        List<BooleanClause> clauses = ((BooleanQuery) q).clauses();
        return clauses.isEmpty() == false && clauses.stream().allMatch(BooleanClause::isProhibited);
    }

    public static Query fixNegativeQueryIfNeeded(Query q) {
        if (isNegativeQuery(q)) {
            BooleanQuery bq = (BooleanQuery) q;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq) {
                builder.add(clause);
            }
            builder.add(newMatchAllQuery(), BooleanClause.Occur.FILTER);
            return builder.build();
        }
        return q;
    }

    public static Query applyMinimumShouldMatch(BooleanQuery query, @Nullable String minimumShouldMatch) {
        if (minimumShouldMatch == null) {
            return query;
        }
        int optionalClauses = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.occur() == BooleanClause.Occur.SHOULD) {
                optionalClauses++;
            }
        }

        int msm = calculateMinShouldMatch(optionalClauses, minimumShouldMatch);
        if (0 < msm) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : query) {
                builder.add(clause);
            }
            builder.setMinimumNumberShouldMatch(msm);
            return builder.build();
        } else {
            return query;
        }
    }

    /**
     * Potentially apply minimum should match value if we have a query that it can be applied to,
     * otherwise return the original query.
     */
    public static Query maybeApplyMinimumShouldMatch(Query query, @Nullable String minimumShouldMatch) {
        if (query instanceof BooleanQuery) {
            return applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        return query;
    }

    private static final Pattern spaceAroundLessThanPattern = Pattern.compile("(\\s+<\\s*)|(\\s*<\\s+)");
    private static final Pattern spacePattern = Pattern.compile(" ");
    private static final Pattern lessThanPattern = Pattern.compile("<");

    public static int calculateMinShouldMatch(int optionalClauseCount, String spec) {
        int result = optionalClauseCount;
        spec = spec.trim();

        if (spec.contains("<")) {
            /* we have conditional spec(s) */
            spec = spaceAroundLessThanPattern.matcher(spec).replaceAll("<");
            for (String s : spacePattern.split(spec)) {
                String[] parts = lessThanPattern.split(s, 0);
                int upperBound = Integer.parseInt(parts[0]);
                if (optionalClauseCount <= upperBound) {
                    return result;
                } else {
                    result = calculateMinShouldMatch(optionalClauseCount, parts[1]);
                }
            }
            return result;
        }

        /* otherwise, simple expression */

        if (-1 < spec.indexOf('%')) {
            /* percentage - assume the % was the last char.  If not, let Integer.parseInt fail. */
            spec = spec.substring(0, spec.length() - 1);
            int percent = Integer.parseInt(spec);
            float calc = (result * percent) * (1 / 100f);
            result = calc < 0 ? result + (int) calc : (int) calc;
        } else {
            int calc = Integer.parseInt(spec);
            result = calc < 0 ? result + calc : calc;
        }

        return Math.max(result, 0);
    }
}
