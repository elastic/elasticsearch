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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 */
public class IndicesTermsFilterCache extends AbstractComponent {

    private static TermsFilterValue NO_TERMS = new TermsFilterValue(0, Queries.MATCH_NO_FILTER);
    private final Client client;
    private final Cache<BytesRef, TermsFilterValue> cache;

    /**
     * This cache will not actually cache any values.
     */
    private final Cache<BytesRef, TermsFilterValue> shardSyncCache;

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
        this.shardSyncCache = CacheBuilder.newBuilder().maximumSize(0).build();
    }

    @Nullable
    public Filter termsFilter(final TermsLookup lookup, boolean cacheLookup, @Nullable CacheKeyFilter.Key cacheKey) throws RuntimeException {
        // TODO: figure out how to inject client into abstract terms lookup
        lookup.setClient(client);

        final Cache<BytesRef, TermsFilterValue> lookupCache;
        if (!cacheLookup) {
            /*
                Use the shardSyncCache which never actually caches a response.  The reason we use this is to prevent
                duplicate lookup requests (ie. from multiple shards on the same machine).  This works because a cache will
                block threads requesting a cache value that is already being loaded.  So the first shard requests a lookup
                value which triggers TermsLookup#getFilter which is  responsible for doing the heavy term gathering
                via GetRequest, Query, etc.  The other shards will request the same lookup value and the cache will
                block those requests until the original request is finished and then send response to all threads
                waiting for the same lookup value.
             */
            lookupCache = shardSyncCache;
        } else {
            lookupCache = cache;
        }

        BytesRef key;
        if (cacheKey != null) {
            key = new BytesRef(cacheKey.bytes());
        } else {
            key = new BytesRef(lookup.toString());
        }

        try {
            return lookupCache.get(key, new Callable<TermsFilterValue>() {
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
        Filter filter = lookup.getFilter();
        if (filter == null) {
            return NO_TERMS;
        }

        return new TermsFilterValue(lookup.estimateSizeInBytes(), filter);
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
