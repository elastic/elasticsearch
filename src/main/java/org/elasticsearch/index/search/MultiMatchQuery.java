/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryParseContext;

import java.util.List;

public class MultiMatchQuery extends MatchQuery {

    private boolean useDisMax = true;
    private int tieBreaker;

    public void setUseDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    public void setTieBreaker(int tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public MultiMatchQuery(QueryParseContext parseContext) {
        super(parseContext);
    }

    public Query parse(Type type, List<String> fieldNames, String text) {
        if (fieldNames.size() == 1) {
            return parse(type, fieldNames.get(0), text);
        }

        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            for (String fieldName : fieldNames) {
                disMaxQuery.add(parse(type, fieldName, text));
            }
            return disMaxQuery;
        } else {
            BooleanQuery booleanQuery = new BooleanQuery();
            for (String fieldName : fieldNames) {
                booleanQuery.add(parse(type, fieldName, text), BooleanClause.Occur.SHOULD);
            }
            return booleanQuery;
        }
    }

}