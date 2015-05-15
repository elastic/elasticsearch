/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MapperService;

/**
 * A Query that matches documents containing a term.
 */
public class TermQueryBuilder extends BaseTermQueryBuilder<TermQueryBuilder> {
    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, String) */
    public TermQueryBuilder(String fieldName, String value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, int) */
    public TermQueryBuilder(String fieldName, int value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, long) */
    public TermQueryBuilder(String fieldName, long value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, float) */
    public TermQueryBuilder(String fieldName, float value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, double) */
    public TermQueryBuilder(String fieldName, double value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, boolean) */
    public TermQueryBuilder(String fieldName, boolean value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, Object) */
    public TermQueryBuilder(String fieldName, Object value) {
        super(fieldName, value);
    }

    public TermQueryBuilder() {
        // for serialization only
    }

    /**
     * @return the field name used in this query
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * @return the value used in this query
     */
    @Override
    public Object value() {
        return this.value;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public TermQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Gets the boost for this query.
     */
    @Override
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the query.
     */
    @Override
    public TermQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    @Override
    public String queryName() {
        return this.queryName;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) {
        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(this.fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().termQuery(this.value, parseContext);
        }
        if (query == null) {
            query = new TermQuery(new Term(this.fieldName, BytesRefs.toBytesRef(this.value)));
        }
        query.setBoost(this.boost);
        if (this.queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    protected String parserName() {
        return TermQueryParser.NAME;
    }

}
