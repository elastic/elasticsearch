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

package org.elasticsearch.indices.cache.filter.terms;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 */
public class IndicesTermsFilterCache extends AbstractComponent {

    private static TermsFilterValue NO_TERMS = new TermsFilterValue(0, null);

    private final Client client;

    private final Cache<CacheKeyFilter.Key, TermsFilterValue> cache;

    @Inject
    public IndicesTermsFilterCache(Settings settings, Client client) {
        super(settings);
        this.client = client;

        ByteSizeValue size = componentSettings.getAsBytesSize("size", new ByteSizeValue(10, ByteSizeUnit.MB));
        TimeValue expireAfterWrite = componentSettings.getAsTime("expire_after_write", null);
        TimeValue expireAfterAccess = componentSettings.getAsTime("expire_after_access", null);

        CacheBuilder<CacheKeyFilter.Key, TermsFilterValue> builder = CacheBuilder.newBuilder()
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

    /**
     * An external lookup terms filter. Note, already implements the {@link CacheKeyFilter} so no need
     * to double cache key it.
     */
    public Filter lookupTermsFilter(final CacheKeyFilter.Key cacheKey, final TermsLookup lookup) {
        return new LookupTermsFilter(lookup, cacheKey, this);
    }

    @Nullable
    private Filter termsFilter(final CacheKeyFilter.Key cacheKey, final TermsLookup lookup) throws RuntimeException {
        try {
            return cache.get(cacheKey, new Callable<TermsFilterValue>() {
                @Override
                public TermsFilterValue call() throws Exception {
                    GetResponse getResponse = client.get(new GetRequest(lookup.getIndex(), lookup.getType(), lookup.getId()).routing(lookup.getRouting()).preference("_local")).actionGet();
                    if (!getResponse.isExists()) {
                        return NO_TERMS;
                    }
                    List<Object> values = XContentMapValues.extractRawValues(lookup.getPath(), getResponse.getSourceAsMap());
                    if (values.isEmpty()) {
                        return NO_TERMS;
                    }
                    Filter filter;
                    if (lookup.getFieldMapper() != null) {
                        filter = lookup.getFieldMapper().termsFilter(values, lookup.getQueryParseContext());
                    } else {
                        BytesRef[] filterValues = new BytesRef[values.size()];
                        for (int i = 0; i < filterValues.length; i++) {
                            filterValues[i] = BytesRefs.toBytesRef(values.get(i));
                        }
                        filter = new TermsFilter(lookup.getFieldName(), filterValues);
                    }
                    return new TermsFilterValue(estimateSizeInBytes(values), filter);
                }
            }).filter;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new ElasticSearchException(e.getMessage(), e.getCause());
        }
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
        final BytesRef spare = new BytesRef();
        for (String key : keys) {
            cache.invalidate(new CacheKeyFilter.Key(Strings.toUTF8Bytes(key, spare)));
        }
    }

    static class LookupTermsFilter extends Filter implements CacheKeyFilter {

        private final TermsLookup lookup;
        private final CacheKeyFilter.Key cacheKey;
        private final IndicesTermsFilterCache cache;
        boolean termsFilterCalled;
        private Filter termsFilter;

        LookupTermsFilter(TermsLookup lookup, CacheKeyFilter.Key cacheKey, IndicesTermsFilterCache cache) {
            this.lookup = lookup;
            this.cacheKey = cacheKey;
            this.cache = cache;
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // only call the terms filter once per execution (across segments per single search request)
            if (!termsFilterCalled) {
                termsFilterCalled = true;
                termsFilter = cache.termsFilter(cacheKey, lookup);
            }
            if (termsFilter == null) return null;
            return termsFilter.getDocIdSet(context, acceptDocs);
        }

        @Override
        public Key cacheKey() {
            return this.cacheKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LookupTermsFilter that = (LookupTermsFilter) o;

            if (!cacheKey.equals(that.cacheKey)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return cacheKey.hashCode();
        }

        @Override
        public String toString() {
            return "terms(" + lookup.toString() + ")";
        }
    }

    static class TermsFilterValueWeigher implements Weigher<CacheKeyFilter.Key, TermsFilterValue> {

        @Override
        public int weigh(CacheKeyFilter.Key key, TermsFilterValue value) {
            return (int) (key.bytes().length + value.sizeInBytes);
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
