/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.Set;

/**
 * Records the reader contexts that the documents of each page read, as the pages leave the coordinator of a remote
 * cluster for the coordinator of the querying cluster. Once its request ends, the coordinator of the remote cluster hands
 * over exactly those contexts, and frees the others its data nodes kept: no row of the querying cluster reads them.
 * <p>
 * The documents arrive detached from the data nodes, so every row names its origin. The sink records a context before it
 * hands the page on, because handing it on gives the page away.
 */
public final class LeavingDocumentsSink implements ExchangeSink {
    private final ExchangeSink delegate;
    private final Set<ShardSearchContextId> read;

    /**
     * @param read collects the contexts the rows of the pages read. Must be safe to add to from several drivers at once.
     */
    public LeavingDocumentsSink(ExchangeSink delegate, Set<ShardSearchContextId> read) {
        this.delegate = delegate;
        this.read = read;
    }

    @Override
    public void addPage(Page page) {
        try {
            record(page);
        } catch (Exception e) {
            page.releaseBlocks();
            throw e;
        }
        delegate.addPage(page);
    }

    private void record(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc) {
                DocVector documents = doc.asVector();
                DocOrigins origins = documents.origins();
                if (origins == null) {
                    // a row of this node would name a reader slot that means nothing on the querying cluster
                    throw new IllegalStateException("documents leave the cluster without their origins");
                }
                IntVector ordinals = documents.shards();
                boolean[] seen = new boolean[origins.size()];
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    int ordinal = ordinals.getInt(p);
                    if (seen[ordinal] == false) {
                        seen[ordinal] = true;
                        read.add(origins.get(ordinal).contextId());
                    }
                }
            }
        }
    }

    @Override
    public void finish() {
        delegate.finish();
    }

    @Override
    public boolean isFinished() {
        return delegate.isFinished();
    }

    @Override
    public void addCompletionListener(ActionListener<Void> listener) {
        delegate.addCompletionListener(listener);
    }

    @Override
    public IsBlockedResult waitForWriting() {
        return delegate.waitForWriting();
    }
}
