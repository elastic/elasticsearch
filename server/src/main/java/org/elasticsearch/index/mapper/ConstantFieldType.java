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

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;

/**
 * A {@link MappedFieldType} that has the same value for all documents.
 * Factory methods for queries are called at rewrite time so they should be
 * cheap. In particular they should not read data from disk or perform a
 * network call. Furthermore they may only return a {@link MatchAllDocsQuery}
 * or a {@link MatchNoDocsQuery}.
 */
public abstract class ConstantFieldType extends MappedFieldType {

    public ConstantFieldType() {
        super();
    }

    public ConstantFieldType(ConstantFieldType other) {
        super(other);
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    @Override
    public final Query existsQuery(QueryShardContext context) {
        return new MatchAllDocsQuery();
    }

}
