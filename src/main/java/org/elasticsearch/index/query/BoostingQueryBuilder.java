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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The BoostingQuery class can be used to effectively demote results that match a given query.
 * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
 * but reduces their overall score:
 * <p/>
 * Query balancedQuery = new BoostingQuery(positiveQuery, negativeQuery, 0.01f);
 * In this scenario the positiveQuery contains the mandatory, desirable criteria which is used to
 * select all matching documents, and the negativeQuery contains the undesirable elements which
 * are simply used to lessen the scores. Documents that match the negativeQuery have their score
 * multiplied by the supplied "boost" parameter, so this should be less than 1 to achieve a
 * demoting effect
 */
public class BoostingQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<BoostingQueryBuilder> {

    private QueryBuilder positiveQuery;

    private QueryBuilder negativeQuery;

    private float negativeBoost = -1;

    private float boost = -1;

    public BoostingQueryBuilder() {

    }

    public BoostingQueryBuilder positive(QueryBuilder positiveQuery) {
        this.positiveQuery = positiveQuery;
        return this;
    }

    public BoostingQueryBuilder negative(QueryBuilder negativeQuery) {
        this.negativeQuery = negativeQuery;
        return this;
    }

    public BoostingQueryBuilder negativeBoost(float negativeBoost) {
        this.negativeBoost = negativeBoost;
        return this;
    }

    @Override
    public BoostingQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (positiveQuery == null) {
            throw new ElasticsearchIllegalArgumentException("boosting query requires positive query to be set");
        }
        if (negativeQuery == null) {
            throw new ElasticsearchIllegalArgumentException("boosting query requires negative query to be set");
        }
        if (negativeBoost == -1) {
            throw new ElasticsearchIllegalArgumentException("boosting query requires negativeBoost to be set");
        }
        builder.startObject(BoostingQueryParser.NAME);
        builder.field("positive");
        positiveQuery.toXContent(builder, params);
        builder.field("negative");
        negativeQuery.toXContent(builder, params);

        builder.field("negative_boost", negativeBoost);

        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}
