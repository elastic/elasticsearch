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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TTLFieldMapper;
import org.elasticsearch.index.mapper.TimestampFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;

/**
 * Abstract base for scrolling across a search and executing bulk indexes on all
 * results.
 */
public abstract class AbstractAsyncBulkIndexByScrollAction<Request extends AbstractBulkByScrollRequest<Request>>
        extends AbstractAsyncBulkByScrollAction<Request> {

    protected final ScriptService scriptService;
    protected final ClusterState clusterState;

    /**
     * This BiFunction is used to apply various changes depending of the Reindex action and  the search hit,
     * from copying search hit metadata (parent, routing, etc) to potentially transforming the
     * {@link RequestWrapper} completely.
     */
    private final BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> scriptApplier;

    public AbstractAsyncBulkIndexByScrollAction(BulkByScrollTask task, Logger logger, ParentTaskAssigningClient client,
                                                ThreadPool threadPool, Request mainRequest,
                                                ActionListener<BulkIndexByScrollResponse> listener,
                                                ScriptService scriptService, ClusterState clusterState) {
        super(task, logger, client, threadPool, mainRequest, listener);
        this.scriptService = scriptService;
        this.clusterState = clusterState;
        this.scriptApplier = Objects.requireNonNull(buildScriptApplier(), "script applier must not be null");
    }

    /**
     * Build the {@link BiFunction} to apply to all {@link RequestWrapper}.
     */
    protected BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
        // The default script applier executes a no-op
        return (request, searchHit) -> request;
    }

    @Override
    protected BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (ScrollableHitSource.Hit doc : docs) {
            if (accept(doc)) {
                RequestWrapper<?> request = scriptApplier.apply(copyMetadata(buildRequest(doc), doc), doc);
                if (request != null) {
                    bulkRequest.add(request.self());
                }
            }
        }
        return bulkRequest;
    }

    /**
     * Used to accept or ignore a search hit. Ignored search hits will be excluded
     * from the bulk request. It is also where we fail on invalid search hits, like
     * when the document has no source but it's required.
     */
    protected boolean accept(ScrollableHitSource.Hit doc) {
        if (doc.getSource() == null) {
            /*
             * Either the document didn't store _source or we didn't fetch it for some reason. Since we don't allow the user to
             * change the "fields" part of the search request it is unlikely that we got here because we didn't fetch _source.
             * Thus the error message assumes that it wasn't stored.
             */
            throw new IllegalArgumentException("[" + doc.getIndex() + "][" + doc.getType() + "][" + doc.getId() + "] didn't store _source");
        }
        return true;
    }

    /**
     * Build the {@link RequestWrapper} for a single search hit. This shouldn't handle
     * metadata or scripting. That will be handled by copyMetadata and
     * apply functions that can be overridden.
     */
    protected abstract RequestWrapper<?> buildRequest(ScrollableHitSource.Hit doc);

    /**
     * Copies the metadata from a hit to the request.
     */
    protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
        request.setParent(doc.getParent());
        copyRouting(request, doc.getRouting());

        // Comes back as a Long but needs to be a string
        Long timestamp = doc.getTimestamp();
        if (timestamp != null) {
            request.setTimestamp(timestamp.toString());
        }
        Long ttl = doc.getTTL();
        if (ttl != null) {
            request.setTtl(ttl);
        }
        return request;
    }

    /**
     * Copy the routing from a search hit to the request.
     */
    protected void copyRouting(RequestWrapper<?> request, String routing) {
        request.setRouting(routing);
    }

    /**
     * Wrapper for the {@link ActionRequest} that are used in this action class.
     */
    interface RequestWrapper<Self extends ActionRequest<Self>> {

        void setIndex(String index);

        String getIndex();

        void setType(String type);

        String getType();

        void setId(String id);

        String getId();

        void setVersion(long version);

        long getVersion();

        void setVersionType(VersionType versionType);

        void setParent(String parent);

        String getParent();

        void setRouting(String routing);

        String getRouting();

        void setTimestamp(String timestamp);

        void setTtl(Long ttl);

        void setSource(Map<String, Object> source);

        Map<String, Object> getSource();

        Self self();
    }

    /**
     * {@link RequestWrapper} for {@link IndexRequest}
     */
    public static class IndexRequestWrapper implements RequestWrapper<IndexRequest> {

        private final IndexRequest request;

        IndexRequestWrapper(IndexRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped IndexRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setType(String type) {
            request.type(type);
        }

        @Override
        public String getType() {
            return request.type();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setParent(String parent) {
            request.parent(parent);
        }

        @Override
        public String getParent() {
            return request.parent();
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public void setTimestamp(String timestamp) {
            request.timestamp(timestamp);
        }

        @Override
        public void setTtl(Long ttl) {
            if (ttl == null) {
                request.ttl((TimeValue) null);
            } else {
                request.ttl(ttl);
            }
        }

        @Override
        public Map<String, Object> getSource() {
            return request.sourceAsMap();
        }

        @Override
        public void setSource(Map<String, Object> source) {
            request.source(source);
        }

        @Override
        public IndexRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link IndexRequest} in a {@link RequestWrapper}
     */
    static RequestWrapper<IndexRequest> wrap(IndexRequest request) {
        return new IndexRequestWrapper(request);
    }

    /**
     * {@link RequestWrapper} for {@link DeleteRequest}
     */
    public static class DeleteRequestWrapper implements RequestWrapper<DeleteRequest> {

        private final DeleteRequest request;

        DeleteRequestWrapper(DeleteRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped DeleteRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setType(String type) {
            request.type(type);
        }

        @Override
        public String getType() {
            return request.type();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setParent(String parent) {
            request.parent(parent);
        }

        @Override
        public String getParent() {
            return request.parent();
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public void setTimestamp(String timestamp) {
            throw new UnsupportedOperationException("unable to set [timestamp] on action request [" + request.getClass() + "]");
        }

        @Override
        public void setTtl(Long ttl) {
            throw new UnsupportedOperationException("unable to set [ttl] on action request [" + request.getClass() + "]");
        }

        @Override
        public Map<String, Object> getSource() {
            throw new UnsupportedOperationException("unable to get source from action request [" + request.getClass() + "]");
        }

        @Override
        public void setSource(Map<String, Object> source) {
            throw new UnsupportedOperationException("unable to set [source] on action request [" + request.getClass() + "]");
        }

        @Override
        public DeleteRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link DeleteRequest} in a {@link RequestWrapper}
     */
    static RequestWrapper<DeleteRequest> wrap(DeleteRequest request) {
        return new DeleteRequestWrapper(request);
    }

    /**
     * Apply a {@link Script} to a {@link RequestWrapper}
     */
    public abstract class ScriptApplier implements BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> {

        private final BulkByScrollTask task;
        private final ScriptService scriptService;
        private final Script script;
        private final Map<String, Object> params;

        private ExecutableScript executable;
        private Map<String, Object> context;

        public ScriptApplier(BulkByScrollTask task, ScriptService scriptService, Script script,
                             Map<String, Object> params) {
            this.task = task;
            this.scriptService = scriptService;
            this.script = script;
            this.params = params;
        }

        @Override
        @SuppressWarnings("unchecked")
        public RequestWrapper<?> apply(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
            if (script == null) {
                return request;
            }
            if (executable == null) {
                CompiledScript compiled = scriptService.compile(script, ScriptContext.Standard.UPDATE, emptyMap());
                executable = scriptService.executable(compiled, params);
            }
            if (context == null) {
                context = new HashMap<>();
            } else {
                context.clear();
            }

            context.put(IndexFieldMapper.NAME, doc.getIndex());
            context.put(TypeFieldMapper.NAME, doc.getType());
            context.put(IdFieldMapper.NAME, doc.getId());
            Long oldVersion = doc.getVersion();
            context.put(VersionFieldMapper.NAME, oldVersion);
            String oldParent = doc.getParent();
            context.put(ParentFieldMapper.NAME, oldParent);
            String oldRouting = doc.getRouting();
            context.put(RoutingFieldMapper.NAME, oldRouting);
            Long oldTimestamp = doc.getTimestamp();
            context.put(TimestampFieldMapper.NAME, oldTimestamp);
            Long oldTTL = doc.getTTL();
            context.put(TTLFieldMapper.NAME, oldTTL);
            context.put(SourceFieldMapper.NAME, request.getSource());

            OpType oldOpType = OpType.INDEX;
            context.put("op", oldOpType.toString());

            executable.setNextVar("ctx", context);
            executable.run();

            Map<String, Object> resultCtx = (Map<String, Object>) executable.unwrap(context);
            String newOp = (String) resultCtx.remove("op");
            if (newOp == null) {
                throw new IllegalArgumentException("Script cleared operation type");
            }

            /*
             * It'd be lovely to only set the source if we know its been modified
             * but it isn't worth keeping two copies of it around just to check!
             */
            request.setSource((Map<String, Object>) resultCtx.remove(SourceFieldMapper.NAME));

            Object newValue = resultCtx.remove(IndexFieldMapper.NAME);
            if (false == doc.getIndex().equals(newValue)) {
                scriptChangedIndex(request, newValue);
            }
            newValue = resultCtx.remove(TypeFieldMapper.NAME);
            if (false == doc.getType().equals(newValue)) {
                scriptChangedType(request, newValue);
            }
            newValue = resultCtx.remove(IdFieldMapper.NAME);
            if (false == doc.getId().equals(newValue)) {
                scriptChangedId(request, newValue);
            }
            newValue = resultCtx.remove(VersionFieldMapper.NAME);
            if (false == Objects.equals(oldVersion, newValue)) {
                scriptChangedVersion(request, newValue);
            }
            newValue = resultCtx.remove(ParentFieldMapper.NAME);
            if (false == Objects.equals(oldParent, newValue)) {
                scriptChangedParent(request, newValue);
            }
            /*
             * Its important that routing comes after parent in case you want to
             * change them both.
             */
            newValue = resultCtx.remove(RoutingFieldMapper.NAME);
            if (false == Objects.equals(oldRouting, newValue)) {
                scriptChangedRouting(request, newValue);
            }
            newValue = resultCtx.remove(TimestampFieldMapper.NAME);
            if (false == Objects.equals(oldTimestamp, newValue)) {
                scriptChangedTimestamp(request, newValue);
            }
            newValue = resultCtx.remove(TTLFieldMapper.NAME);
            if (false == Objects.equals(oldTTL, newValue)) {
                scriptChangedTTL(request, newValue);
            }

            OpType newOpType = OpType.fromString(newOp);
            if (newOpType != oldOpType) {
                return scriptChangedOpType(request, oldOpType, newOpType);
            }

            if (false == resultCtx.isEmpty()) {
                throw new IllegalArgumentException("Invalid fields added to context [" + String.join(",", resultCtx.keySet()) + ']');
            }
            return request;
        }

        protected RequestWrapper<?> scriptChangedOpType(RequestWrapper<?> request, OpType oldOpType, OpType newOpType) {
            switch (newOpType) {
            case NOOP:
                task.countNoop();
                return null;
            case DELETE:
                RequestWrapper<DeleteRequest> delete = wrap(new DeleteRequest(request.getIndex(), request.getType(), request.getId()));
                delete.setVersion(request.getVersion());
                delete.setVersionType(VersionType.INTERNAL);
                delete.setParent(request.getParent());
                delete.setRouting(request.getRouting());
                return delete;
            default:
                throw new IllegalArgumentException("Unsupported operation type change from [" + oldOpType + "] to [" + newOpType + "]");
            }
        }

        protected abstract void scriptChangedIndex(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedType(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedId(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedVersion(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedRouting(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedParent(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedTimestamp(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedTTL(RequestWrapper<?> request, Object to);

    }

    public enum OpType {

        NOOP("noop"),
        INDEX("index"),
        DELETE("delete");

        private final String id;

        OpType(String id) {
            this.id = id;
        }

        public static OpType fromString(String opType) {
            String lowerOpType = opType.toLowerCase(Locale.ROOT);
            switch (lowerOpType) {
                case "noop":
                    return OpType.NOOP;
                case "index":
                    return OpType.INDEX;
                case "delete":
                    return OpType.DELETE;
                default:
                    throw new IllegalArgumentException("Operation type [" + lowerOpType + "] not allowed, only " +
                            Arrays.toString(values()) + " are allowed");
            }
        }

        @Override
        public String toString() {
            return id.toLowerCase(Locale.ROOT);
        }
    }
}
