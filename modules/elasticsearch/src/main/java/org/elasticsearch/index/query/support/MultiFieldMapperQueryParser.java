/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query.support;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.elasticsearch.index.query.xcontent.QueryParseContext;
import org.elasticsearch.util.trove.ExtTObjectFloatHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class MultiFieldMapperQueryParser extends MapperQueryParser {

    private List<String> fields;

    private ExtTObjectFloatHashMap<String> boosts;

    private float tieBreaker = 0.0f;

    private boolean useDisMax = true;

    public MultiFieldMapperQueryParser(List<String> fields, @Nullable ExtTObjectFloatHashMap<String> boosts, Analyzer analyzer, QueryParseContext parseContext) {
        super(null, analyzer, parseContext);
        this.fields = fields;
        this.boosts = boosts;
        if (this.boosts != null) {
            boosts.defaultReturnValue(1.0f);
        }
    }

    public void setTieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public void setUseDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    @Override public Query getFieldQuery(String field, String queryText) throws ParseException {
        return getFieldQuery(field, queryText, 0);
    }

    @Override public Query getFieldQuery(String xField, String queryText, int slop) throws ParseException {
        if (xField != null) {
            Query q = super.getFieldQuery(xField, queryText);
            applySlop(q, slop);
            return q;
        }
        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean added = false;
            for (String field : fields) {
                Query q = super.getFieldQuery(field, queryText);
                if (q != null) {
                    added = true;
                    applyBoost(field, q);
                    applySlop(q, slop);
                    disMaxQuery.add(q);
                }
            }
            if (!added) {
                return null;
            }
            return disMaxQuery;
        } else {
            List<BooleanClause> clauses = new ArrayList<BooleanClause>();
            for (String field : fields) {
                Query q = super.getFieldQuery(field, queryText);
                if (q != null) {
                    applyBoost(field, q);
                    applySlop(q, slop);
                    clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
                }
            }
            if (clauses.size() == 0)  // happens for stopwords
                return null;
            return getBooleanQuery(clauses, true);
        }
    }

    @Override protected Query getRangeQuery(String xField, String part1, String part2, boolean inclusive) throws ParseException {
        if (xField != null) {
            return super.getRangeQuery(xField, part1, part2, inclusive);
        }
        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean added = false;
            for (String field : fields) {
                Query q = super.getRangeQuery(field, part1, part2, inclusive);
                if (q != null) {
                    added = true;
                    applyBoost(field, q);
                    disMaxQuery.add(q);
                }
            }
            if (!added) {
                return null;
            }
            return disMaxQuery;
        } else {
            List<BooleanClause> clauses = new ArrayList<BooleanClause>();
            for (String field : fields) {
                Query q = super.getRangeQuery(field, part1, part2, inclusive);
                if (q != null) {
                    applyBoost(field, q);
                    clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
                }
            }
            if (clauses.size() == 0)  // happens for stopwords
                return null;
            return getBooleanQuery(clauses, true);
        }
    }

    @Override protected Query getPrefixQuery(String xField, String termStr) throws ParseException {
        if (xField != null) {
            return super.getPrefixQuery(xField, termStr);
        }
        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean added = false;
            for (String field : fields) {
                Query q = super.getPrefixQuery(field, termStr);
                if (q != null) {
                    added = true;
                    applyBoost(field, q);
                    disMaxQuery.add(q);
                }
            }
            if (!added) {
                return null;
            }
            return disMaxQuery;
        } else {
            List<BooleanClause> clauses = new ArrayList<BooleanClause>();
            for (String field : fields) {
                Query q = super.getPrefixQuery(field, termStr);
                if (q != null) {
                    applyBoost(field, q);
                    clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
                }
            }
            if (clauses.size() == 0)  // happens for stopwords
                return null;
            return getBooleanQuery(clauses, true);
        }
    }

    @Override protected Query getWildcardQuery(String xField, String termStr) throws ParseException {
        if (xField != null) {
            return super.getWildcardQuery(xField, termStr);
        }
        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean added = false;
            for (String field : fields) {
                Query q = super.getWildcardQuery(field, termStr);
                if (q != null) {
                    added = true;
                    applyBoost(field, q);
                    disMaxQuery.add(q);
                }
            }
            if (!added) {
                return null;
            }
            return disMaxQuery;
        } else {
            List<BooleanClause> clauses = new ArrayList<BooleanClause>();
            for (String field : fields) {
                Query q = super.getWildcardQuery(field, termStr);
                if (q != null) {
                    applyBoost(field, q);
                    clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
                }
            }
            if (clauses.size() == 0)  // happens for stopwords
                return null;
            return getBooleanQuery(clauses, true);
        }
    }

    @Override protected Query getFuzzyQuery(String xField, String termStr, float minSimilarity) throws ParseException {
        if (xField != null) {
            return super.getFuzzyQuery(xField, termStr, minSimilarity);
        }
        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean added = false;
            for (String field : fields) {
                Query q = super.getFuzzyQuery(field, termStr, minSimilarity);
                if (q != null) {
                    added = true;
                    applyBoost(field, q);
                    disMaxQuery.add(q);
                }
            }
            if (!added) {
                return null;
            }
            return disMaxQuery;
        } else {
            List<BooleanClause> clauses = new ArrayList<BooleanClause>();
            for (String field : fields) {
                Query q = super.getFuzzyQuery(field, termStr, minSimilarity);
                applyBoost(field, q);
                clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
            }
            return getBooleanQuery(clauses, true);
        }
    }

    private void applyBoost(String field, Query q) {
        if (boosts != null) {
            float boost = boosts.get(field);
            q.setBoost(boost);
        }
    }

    private void applySlop(Query q, int slop) {
        if (q instanceof PhraseQuery) {
            ((PhraseQuery) q).setSlop(slop);
        } else if (q instanceof MultiPhraseQuery) {
            ((MultiPhraseQuery) q).setSlop(slop);
        }
    }
}
