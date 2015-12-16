package org.elasticsearch.plugin.indexbysearch;

import java.util.HashMap;
import java.util.Map;
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
                    scriptCtx = new HashMap<>(2);
                }
                if (applyScript(index, executableScript, scriptCtx) == false) {
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
        SearchHitField parent = doc.field("_parent");
        if (parent != null) {
            index.parent(parent.value());
        }
        copyRouting(index, doc);
        SearchHitField timestamp = doc.field("_timestamp");
        if (timestamp != null) {
            // Comes back as a Long but needs to be a string
            index.timestamp(timestamp.value().toString());
        }
        SearchHitField ttl = doc.field("_ttl");
        if (ttl != null) {
            index.ttl(ttl.value());
        }
    }

    /**
     * Part of copyMetadata but called out individual for easy overwriting.
     */
    protected void copyRouting(IndexRequest index, SearchHit doc) {
        SearchHitField routing = doc.field("_routing");
        if (routing != null) {
            index.routing(routing.value());
        }
    }

    /**
     * Apply a script to the request.
     *
     * @return is this request still ok to apply (true) or is it a noop (false)
     */
    @SuppressWarnings("unchecked")
    protected boolean applyScript(IndexRequest index, ExecutableScript script, Map<String, Object> ctx) {
        if (script == null) {
            return true;
        }
        ctx.put("_source", index.sourceAsMap());
        ctx.put("op", "update");
        script.setNextVar("ctx", ctx);
        script.run();
        ctx = (Map<String, Object>) script.unwrap(ctx);
        String newOp = (String) ctx.get("op");
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
        Map<String, Object> newSource = (Map<String, Object>) ctx.get("_source");
        index.source(newSource);
        return true;
    }
}
