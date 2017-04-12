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

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 */
public class QueryRewriteContext {
    protected final MapperService mapperService;
    protected final ScriptService scriptService;
    protected final IndexSettings indexSettings;
    private final NamedXContentRegistry xContentRegistry;
    protected final Client client;
    protected final IndexReader reader;
    protected final LongSupplier nowInMillis;

    public QueryRewriteContext(IndexSettings indexSettings, MapperService mapperService, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, Client client, IndexReader reader,
            LongSupplier nowInMillis) {
        this.mapperService = mapperService;
        this.scriptService = scriptService;
        this.indexSettings = indexSettings;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.reader = reader;
        this.nowInMillis = nowInMillis;
    }

    /**
     * Returns a clients to fetch resources from local or remove nodes.
     */
    public Client getClient() {
        return client;
    }

    /**
     * Returns the index settings for this context. This might return null if the
     * context has not index scope.
     */
    public final IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Return the MapperService.
     */
    public final MapperService getMapperService() {
        return mapperService;
    }

    /** Return the current {@link IndexReader}, or {@code null} if no index reader is available, for
     *  instance if we are on the coordinating node or if this rewrite context is used to index
     *  queries (percolation). */
    public IndexReader getIndexReader() {
        return reader;
    }

    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    /**
     * Returns a new {@link QueryParseContext} that wraps the provided parser.
     */
    public QueryParseContext newParseContext(XContentParser parser) {
        return new QueryParseContext(parser);
    }

    public long nowInMillis() {
        return nowInMillis.getAsLong();
    }

    public BytesReference getTemplateBytes(Script template) {
        ExecutableScript executable = scriptService.executable(template, ScriptContext.Standard.SEARCH);
        return (BytesReference) executable.run();
    }
}
