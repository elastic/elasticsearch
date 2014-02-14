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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 */
public class IndicesTermsFilterCache extends AbstractComponent {

    private static TermsFilterValue NO_TERMS = new TermsFilterValue(0, Queries.MATCH_NO_FILTER);

    private final Client client;

    private final Cache<BytesRef, TermsFilterValue> cache;

    @Inject
    public IndicesTermsFilterCache(Settings settings, Client client) {
        super(settings);
        this.client = client;

        ByteSizeValue size = componentSettings.getAsBytesSize("size", new ByteSizeValue(10, ByteSizeUnit.MB));
        TimeValue expireAfterWrite = componentSettings.getAsTime("expire_after_write", null);
        TimeValue expireAfterAccess = componentSettings.getAsTime("expire_after_access", null);

        CacheBuilder<BytesRef, TermsFilterValue> builder = CacheBuilder.newBuilder()
                .maximumWeight(size.bytes())
                .weigher(new TermsFilterValueWeigher());

        if (expireAfterAccess != null) {
            builder.expireAfterAccess(expireAfterAccess.millis(), TimeUnit.MILLISECONDS);
        }
        if (expireAfterWrite != null) {
            builder.expireAfterWrite(expireAfterWrite.millis(), TimeUnit.MILLISECONDS);
        }

        this.cache = builder.build();
    }

    @Nullable
    public Filter termsFilter(final TermsLookup lookup, boolean cacheLookup, @Nullable CacheKeyFilter.Key cacheKey) throws RuntimeException {
        if (!cacheLookup) {
            return buildTermsFilterValue(lookup).filter;
        }

        BytesRef key;
        if (cacheKey != null) {
            key = new BytesRef(cacheKey.bytes());
        } else {
            key = new BytesRef(lookup.toString());
        }
        try {
            return cache.get(key, new Callable<TermsFilterValue>() {
                @Override
                public TermsFilterValue call() throws Exception {
                    return buildTermsFilterValue(lookup);
                }
            }).filter;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new ElasticsearchException(e.getMessage(), e.getCause());
        }
    }

    TermsFilterValue buildTermsFilterValue(TermsLookup lookup) {
        GetResponse getResponse = client.get(new GetRequest(lookup.getIndex(), lookup.getType(), lookup.getId()).preference("_local").routing(lookup.getRouting())).actionGet();
        if (!getResponse.isExists()) {
            return NO_TERMS;
        }
        List<Object> values = XContentMapValues.extractRawValues(lookup.getPath(), getResponse.getSourceAsMap());
        if (values.isEmpty()) {
            return NO_TERMS;
        }
        Filter filter = lookup.getFieldMapper().termsFilter(values, lookup.getQueryParseContext());
        return new TermsFilterValue(estimateSizeInBytes(values), filter);
    }

    long estimateSizeInBytes(List<Object> terms) {
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

    public void clear(String reason) {
        cache.invalidateAll();
    }

    public void clear(String reason, String[] keys) {
        for (String key : keys) {
            cache.invalidate(new BytesRef(key));
        }
    }

    static class TermsFilterValueWeigher implements Weigher<BytesRef, TermsFilterValue> {

        @Override
        public int weigh(BytesRef key, TermsFilterValue value) {
            return (int) (key.length + value.sizeInBytes);
        }
    }

    // TODO: if TermsFilter exposed sizeInBytes, we won't need this wrapper
    static class TermsFilterValue {
        public final long sizeInBytes;
        public final Filter filter;

        TermsFilterValue(long sizeInBytes, Filter filter) {
            this.sizeInBytes = sizeInBytes;
            this.filter = filter;
        }
    }
}
