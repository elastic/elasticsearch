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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;

/**
 * Abstract base for scrolling across a search and executing bulk indexes on all
 * results.
 */
public abstract class AbstractAsyncBulkIndexByScrollAction<
                Request extends AbstractBulkIndexByScrollRequest<Request>,
                Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkByScrollAction<Request, Response> {

    private final ScriptService scriptService;
    private final CompiledScript script;

    public AbstractAsyncBulkIndexByScrollAction(BulkByScrollTask task, ESLogger logger, ScriptService scriptService,
            ClusterState state, Client client, ThreadPool threadPool, Request mainRequest, SearchRequest firstSearchRequest,
            ActionListener<Response> listener) {
        super(task, logger, client, threadPool, mainRequest, firstSearchRequest, listener);
        this.scriptService = scriptService;
        if (mainRequest.getScript() == null) {
            script = null;
        } else {
            script = scriptService.compile(mainRequest.getScript(), ScriptContext.Standard.UPDATE, emptyMap(), state);
        }
    }

    /**
     * Build the IndexRequest for a single search hit. This shouldn't handle
     * metadata or the script. That will be handled by copyMetadata and
     * applyScript functions that can be overridden.
     */
    protected abstract IndexRequest buildIndexRequest(SearchHit doc);

    @Override
    protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
        BulkRequest bulkRequest = new BulkRequest();
        ExecutableScript executableScript = null;
        Map<String, Object> scriptCtx = null;

        for (SearchHit doc : docs) {
            if (doc.hasSource()) {
                /*
                 * Either the document didn't store _source or we didn't fetch it for some reason. Since we don't allow the user to
                 * change the "fields" part of the search request it is unlikely that we got here because we didn't fetch _source.
                 * Thus the error message assumes that it wasn't stored.
                 */
                throw new IllegalArgumentException("[" + doc.index() + "][" + doc.type() + "][" + doc.id() + "] didn't store _source");
            }
            IndexRequest index = buildIndexRequest(doc);
            copyMetadata(index, doc);
            if (script != null) {
                if (executableScript == null) {
                    executableScript = scriptService.executable(script, mainRequest.getScript().getParams());
                    scriptCtx = new HashMap<>();
                }
                if (false == applyScript(index, doc, executableScript, scriptCtx)) {
                    continue;
                }
            }
            bulkRequest.add(index);
        }

        return bulkRequest;
    }

    /**
     * Copies the metadata from a hit to the index request.
     */
    protected void copyMetadata(IndexRequest index, SearchHit doc) {
        index.parent(fieldValue(doc, ParentFieldMapper.NAME));
        copyRouting(index, doc);
        // Comes back as a Long but needs to be a string
        Long timestamp = fieldValue(doc, TimestampFieldMapper.NAME);
        if (timestamp != null) {
            index.timestamp(timestamp.toString());
        }
        Long ttl = fieldValue(doc, TTLFieldMapper.NAME);
        if (ttl != null) {
            index.ttl(ttl);
        }
    }

    /**
     * Part of copyMetadata but called out individual for easy overwriting.
     */
    protected void copyRouting(IndexRequest index, SearchHit doc) {
        index.routing(fieldValue(doc, RoutingFieldMapper.NAME));
    }

    protected <T> T fieldValue(SearchHit doc, String fieldName) {
        SearchHitField field = doc.field(fieldName);
        return field == null ? null : field.value();
    }

    /**
     * Apply a script to the request.
     *
     * @return is this request still ok to apply (true) or is it a noop (false)
     */
    @SuppressWarnings("unchecked")
    protected boolean applyScript(IndexRequest index, SearchHit doc, ExecutableScript script, final Map<String, Object> ctx) {
        if (script == null) {
            return true;
        }
        ctx.put(IndexFieldMapper.NAME, doc.index());
        ctx.put(TypeFieldMapper.NAME, doc.type());
        ctx.put(IdFieldMapper.NAME, doc.id());
        Long oldVersion = doc.getVersion();
        ctx.put(VersionFieldMapper.NAME, oldVersion);
        String oldParent = fieldValue(doc, ParentFieldMapper.NAME);
        ctx.put(ParentFieldMapper.NAME, oldParent);
        String oldRouting = fieldValue(doc, RoutingFieldMapper.NAME);
        ctx.put(RoutingFieldMapper.NAME, oldRouting);
        Long oldTimestamp = fieldValue(doc, TimestampFieldMapper.NAME);
        ctx.put(TimestampFieldMapper.NAME, oldTimestamp);
        Long oldTTL = fieldValue(doc, TTLFieldMapper.NAME);
        ctx.put(TTLFieldMapper.NAME, oldTTL);
        ctx.put(SourceFieldMapper.NAME, index.sourceAsMap());
        ctx.put("op", "update");
        script.setNextVar("ctx", ctx);
        script.run();
        Map<String, Object> resultCtx = (Map<String, Object>) script.unwrap(ctx);
        String newOp = (String) resultCtx.remove("op");
        if (newOp == null) {
            throw new IllegalArgumentException("Script cleared op!");
        }
        if ("noop".equals(newOp)) {
            task.countNoop();
            return false;
        }
        if (false == "update".equals(newOp)) {
            throw new IllegalArgumentException("Invalid op [" + newOp + ']');
        }

        /*
         * It'd be lovely to only set the source if we know its been modified
         * but it isn't worth keeping two copies of it around just to check!
         */
        index.source((Map<String, Object>) resultCtx.remove(SourceFieldMapper.NAME));

        Object newValue = ctx.remove(IndexFieldMapper.NAME);
        if (false == doc.index().equals(newValue)) {
            scriptChangedIndex(index, newValue);
        }
        newValue = ctx.remove(TypeFieldMapper.NAME);
        if (false == doc.type().equals(newValue)) {
            scriptChangedType(index, newValue);
        }
        newValue = ctx.remove(IdFieldMapper.NAME);
        if (false == doc.id().equals(newValue)) {
            scriptChangedId(index, newValue);
        }
        newValue = ctx.remove(VersionFieldMapper.NAME);
        if (false == Objects.equals(oldVersion, newValue)) {
            scriptChangedVersion(index, newValue);
        }
        newValue = ctx.remove(ParentFieldMapper.NAME);
        if (false == Objects.equals(oldParent, newValue)) {
            scriptChangedParent(index, newValue);
        }
        /*
         * Its important that routing comes after parent in case you want to
         * change them both.
         */
        newValue = ctx.remove(RoutingFieldMapper.NAME);
        if (false == Objects.equals(oldRouting, newValue)) {
            scriptChangedRouting(index, newValue);
        }
        newValue = ctx.remove(TimestampFieldMapper.NAME);
        if (false == Objects.equals(oldTimestamp, newValue)) {
            scriptChangedTimestamp(index, newValue);
        }
        newValue = ctx.remove(TTLFieldMapper.NAME);
        if (false == Objects.equals(oldTTL, newValue)) {
            scriptChangedTTL(index, newValue);
        }
        if (false == ctx.isEmpty()) {
            throw new IllegalArgumentException("Invalid fields added to ctx [" + String.join(",", ctx.keySet()) + ']');
        }
        return true;
    }

    protected abstract void scriptChangedIndex(IndexRequest index, Object to);

    protected abstract void scriptChangedType(IndexRequest index, Object to);

    protected abstract void scriptChangedId(IndexRequest index, Object to);

    protected abstract void scriptChangedVersion(IndexRequest index, Object to);

    protected abstract void scriptChangedRouting(IndexRequest index, Object to);

    protected abstract void scriptChangedParent(IndexRequest index, Object to);

    protected abstract void scriptChangedTimestamp(IndexRequest index, Object to);

    protected abstract void scriptChangedTTL(IndexRequest index, Object to);
}
