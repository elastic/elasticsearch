package org.elasticsearch.plugin.reindex;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

/**
 * Abstract base for scrolling across a search and executing bulk indexes on all
 * results.
 */
public abstract class AbstractAsyncBulkIndexByScrollAction<Request extends AbstractBulkIndexByScrollRequest<Request>, Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkByScrollAction<Request, Response> {

    private final AtomicLong noops = new AtomicLong(0);
    private final ScriptService scriptService;
    private final CompiledScript script;

    public AbstractAsyncBulkIndexByScrollAction(ESLogger logger, ScriptService scriptService, Client client, Request mainRequest, SearchRequest firstSearchRequest,
            ActionListener<Response> listener) {
        super(logger, client, mainRequest, firstSearchRequest, listener);
        this.scriptService = scriptService;
        if (mainRequest.script() == null) {
            script = null;
        } else {
            script = scriptService.compile(mainRequest.script(), ScriptContext.Standard.UPDATE, mainRequest);
        }
    }

    /**
     * Build the IndexRequest for a single search hit. This shouldn't handle
     * metadata or the script. That will be handled by copyMetadata and
     * applyScript functions that can be overridden.
     */
    protected abstract IndexRequest buildIndexRequest(SearchHit doc);

    /**
     * The number of noops (skipped bulk items) as part of this request.
     */
    public long noops() {
        return noops.get();
    }

    @Override
    protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
        BulkRequest bulkRequest = new BulkRequest(mainRequest);
        ExecutableScript executableScript = null;
        Map<String, Object> scriptCtx = null;

        for (SearchHit doc : docs) {
            IndexRequest index = buildIndexRequest(doc);
            copyMetadata(index, doc);
            if (script != null) {
                if (executableScript == null) {
                    executableScript = scriptService.executable(script, mainRequest.script().getParams());
                    scriptCtx = new HashMap<>(16);
                }
                if (applyScript(index, doc, executableScript, scriptCtx) == false) {
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
        index.parent(fieldValue(doc, "_parent"));
        copyRouting(index, doc);
        // Comes back as a Long but needs to be a string
        Long timestamp = fieldValue(doc, "_timestamp");
        if (timestamp != null) {
            index.timestamp(timestamp.toString());
        }
        index.ttl(fieldValue(doc, "_ttl"));
    }

    /**
     * Part of copyMetadata but called out individual for easy overwriting.
     */
    protected void copyRouting(IndexRequest index, SearchHit doc) {
        index.routing(fieldValue(doc, "_routing"));
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
        ctx.put("_index", doc.index());
        ctx.put("_type", doc.type());
        ctx.put("_id", doc.id());
        ctx.put("_version", doc.getVersion());
        String oldParent = fieldValue(doc, "_parent");
        ctx.put("_parent", oldParent);
        String oldRouting = fieldValue(doc, "_routing");
        ctx.put("_routing", oldRouting);
        Long oldTimestamp = fieldValue(doc, "_timestamp");
        ctx.put("_timestamp", oldTimestamp);
        Long oldTtl = fieldValue(doc, "_ttl");
        ctx.put("_ttl", oldTtl);
        ctx.put("_source", index.sourceAsMap());
        ctx.put("op", "update");
        script.setNextVar("ctx", ctx);
        script.run();
        Map<String, Object> resultCtx = (Map<String, Object>) script.unwrap(ctx);
        String newOp = (String) resultCtx.get("op");
        if (newOp == null) {
            throw new IllegalArgumentException("Script cleared op!");
        }
        if ("noop".equals(newOp)) {
            noops.incrementAndGet();
            return false;
        }
        if ("update".equals(newOp) == false) {
            throw new IllegalArgumentException("Invalid op [" + newOp + ']');
        }

        /*
         * It'd be lovely to only set the source if we know its been
         * modified but it isn't worth keeping two copies of it
         * around just to check!
         */
        index.source((Map<String, Object>) resultCtx.get("_source"));

        if (false == doc.index().equals(ctx.get("_index"))) {
            throw new IllegalArgumentException("Modifying _index not allowed.");
        }
        if (false == doc.type().equals(ctx.get("_type"))) {
            throw new IllegalArgumentException("Modifying _type not allowed.");
        }
        if (false == doc.id().equals(ctx.get("_id"))) {
            throw new IllegalArgumentException("Modifying _id not allowed.");
        }
        Long version = (Long) ctx.get("_version");
        if (version == null || version != doc.version()) {
            throw new IllegalArgumentException("Modifying _version not allowed.");
        }
        String newParent = (String) ctx.get("_parent");
        if (false == Objects.equals(oldParent, newParent)) {
            scriptChangedParent(index, oldParent, newParent);
        }
        /*
         * Its important that routing comes after parent in case you want to
         * change them both.
         */
        String newRouting = (String) ctx.get("_routing");
        if (false == Objects.equals(oldRouting, newRouting)) {
            scriptChangedRouting(index, oldRouting, newRouting);
        }
        Long newTimestamp = (Long) ctx.get("_timestamp");
        if (false == Objects.equals(oldTimestamp, newTimestamp)) {
            throw new IllegalArgumentException("Modifying _timestamp not allowed.");
        }
        Object newTtl = ctx.get("_ttl");
        if (false == Objects.equals(oldTtl, newTtl)) {
            throw new IllegalArgumentException("Modifying _ttl not allowed.");
        }
        return true;
    }

    protected void scriptChangedRouting(IndexRequest index, String from, String to) {
        throw new IllegalArgumentException("Modifying _routing not allowed.");
    }

    protected void scriptChangedParent(IndexRequest index, String from, String to) {
        throw new IllegalArgumentException("Modifying _parent not allowed.");
    }
}
