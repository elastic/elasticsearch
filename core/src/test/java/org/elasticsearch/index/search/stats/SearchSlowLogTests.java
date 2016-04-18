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

package org.elasticsearch.index.search.stats;

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.HasContext;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.HasHeaders;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.startsWith;

public class SearchSlowLogTests extends ESSingleNodeTestCase {
    protected static SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.injector().getInstance(BigArrays.class);
        ThreadPool threadPool = indexService.injector().getInstance(ThreadPool.class);
        PageCacheRecycler pageCacheRecycler = indexService.injector().getInstance(PageCacheRecycler.class);
        return new TestSearchContext(threadPool, pageCacheRecycler, bigArrays, indexService) {
            @Override
            public ShardSearchRequest request() {
                return new ShardSearchRequest() {
                    @Override
                    public String index() {
                        return null;
                    }

                    @Override
                    public int shardId() {
                        return 0;
                    }

                    @Override
                    public String[] types() {
                        return new String[0];
                    }

                    @Override
                    public BytesReference source() {
                        return null;
                    }

                    @Override
                    public void source(BytesReference source) {

                    }

                    @Override
                    public BytesReference extraSource() {
                        return null;
                    }

                    @Override
                    public int numberOfShards() {
                        return 0;
                    }

                    @Override
                    public SearchType searchType() {
                        return null;
                    }

                    @Override
                    public String[] filteringAliases() {
                        return new String[0];
                    }

                    @Override
                    public long nowInMillis() {
                        return 0;
                    }

                    @Override
                    public Template template() {
                        return null;
                    }

                    @Override
                    public BytesReference templateSource() {
                        return null;
                    }

                    @Override
                    public Boolean requestCache() {
                        return null;
                    }

                    @Override
                    public Scroll scroll() {
                        return null;
                    }

                    @Override
                    public void setProfile(boolean profile) {

                    }

                    @Override
                    public boolean isProfile() {
                        return false;
                    }

                    @Override
                    public BytesReference cacheKey() throws IOException {
                        return null;
                    }

                    @Override
                    public void copyContextAndHeadersFrom(HasContextAndHeaders other) {

                    }

                    @Override
                    public <V> V putInContext(Object key, Object value) {
                        return null;
                    }

                    @Override
                    public void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {

                    }

                    @Override
                    public <V> V getFromContext(Object key) {
                        return null;
                    }

                    @Override
                    public <V> V getFromContext(Object key, V defaultValue) {
                        return null;
                    }

                    @Override
                    public boolean hasInContext(Object key) {
                        return false;
                    }

                    @Override
                    public int contextSize() {
                        return 0;
                    }

                    @Override
                    public boolean isContextEmpty() {
                        return false;
                    }

                    @Override
                    public ImmutableOpenMap<Object, Object> getContext() {
                        return null;
                    }

                    @Override
                    public void copyContextFrom(HasContext other) {

                    }

                    @Override
                    public <V> void putHeader(String key, V value) {

                    }

                    @Override
                    public <V> V getHeader(String key) {
                        return null;
                    }

                    @Override
                    public boolean hasHeader(String key) {
                        return false;
                    }

                    @Override
                    public Set<String> getHeaders() {
                        return null;
                    }

                    @Override
                    public void copyHeadersFrom(HasHeaders from) {

                    }
                };
            }
        };
    }

    public void testSlowLogSearchContextPrinterToLog() throws IOException {
        IndexService index = createIndex("foo");
        SearchContext searchContext = createSearchContext(index);
        SearchSlowLog.SlowLogSearchContextPrinter p = new SearchSlowLog.SlowLogSearchContextPrinter(searchContext, 10, true);
        assertThat(p.toString(), startsWith(index.index().toString()));
    }
}