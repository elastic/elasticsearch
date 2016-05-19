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

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.QueryShardContext;

/** Base {@link MappedFieldType} implementation for a field that is indexed
 *  with the inverted index. */
public abstract class TermBasedFieldType extends MappedFieldType {

    public TermBasedFieldType() {}

    protected TermBasedFieldType(MappedFieldType ref) {
        super(ref);
    }

    /** Returns the indexed value used to construct search "values".
     *  This method is used for the default implementations of most
     *  query factory methods such as {@link #termQuery}. */
    protected BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        failIfNotIndexed();
        TermQuery query = new TermQuery(new Term(name(), indexedValueForSearch(value)));
        if (boost() == 1f ||
            (context != null && context.indexVersionCreated().before(Version.V_5_0_0_alpha1))) {
            return query;
        }
        return new BoostQuery(query, boost());
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        failIfNotIndexed();
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermsQuery(name(), bytesRefs);
    }

}
