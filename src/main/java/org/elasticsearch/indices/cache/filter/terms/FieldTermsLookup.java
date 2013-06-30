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

package org.elasticsearch.indices.cache.filter.terms;

import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.util.List;

/**
 * A {@link TermsLookup} implementation that gathers the filter terms from the field of another document.
 */
public class FieldTermsLookup extends TermsLookup {

    private final FieldMapper fieldMapper;
    private final String index;
    private final String type;
    private final String id;
    private final String routing;
    private final String path;
    @Nullable
    private final QueryParseContext queryParseContext;
    private List<Object> terms;

    public FieldTermsLookup(FieldMapper fieldMapper, String index, String type, String id, String routing, String path,
                            @Nullable QueryParseContext queryParseContext) {
        this.fieldMapper = fieldMapper;
        this.index = index;
        this.type = type;
        this.id = id;
        this.routing = routing;
        this.path = path;
        this.queryParseContext = queryParseContext;
    }

    /**
     * Performs a {@link GetRequest} for the document containing the lookup terms, extracts the terms from the specified
     * field and generates an {@link org.apache.lucene.queries.TermsFilter}.
     * @return the generated filter
     */
    @Override
    public Filter getFilter() {
        GetResponse getResponse = client.get(new GetRequest(index, type, id).preference("_local").routing(routing)).actionGet();
        if (!getResponse.isExists()) {
            return null;
        }

        terms = XContentMapValues.extractRawValues(path, getResponse.getSourceAsMap());
        if (terms.isEmpty()) {
            return null;
        }

        Filter filter = fieldMapper.termsFilter(terms, queryParseContext);
        return filter;
    }

    /**
     * Estimates the size of the filter.
     * @return the estimated size in bytes.
     */
    @Override
    public long estimateSizeInBytes() {
        long size = 8;
        for (Object term : terms) {
            if (term instanceof BytesRef) {
                size += ((BytesRef) term).length;
            } else if (term instanceof String) {
                size += ((String) term).length() / 2;
            } else {
                size += 4;
            }
        }

        return size;
    }

    @Override
    public String toString() {
        return fieldMapper.names().fullName() + ":" + index + "/" + type + "/" + id + "/" + path;
    }
}
