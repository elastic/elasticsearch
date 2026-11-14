/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;

import java.util.function.Function;

/**
 * Tells the execution of a query which fetch contexts the data nodes keep, as the pages with their documents reach the
 * coordinator. A data node keeps every context that the origins of a page that left it name, so the source records each
 * of them, whether or not a row of the page reads it.
 * <p>
 * The coordinator only learns of the pages it reads. When the exchange finishes early, the contexts of the pages it never
 * read wait for the reaper of their node.
 */
public final class ArrivingDocumentsSource implements ExchangeSource {
    private final ExchangeSource delegate;
    private final FetchContextLease.Execution fetchContexts;
    private final Function<DocOrigin, FetchNode> nodeOf;

    /**
     * @param nodeOf the node that holds the context of an origin, as this coordinator reaches it
     */
    public ArrivingDocumentsSource(
        ExchangeSource delegate,
        FetchContextLease.Execution fetchContexts,
        Function<DocOrigin, FetchNode> nodeOf
    ) {
        this.delegate = delegate;
        this.fetchContexts = fetchContexts;
        this.nodeOf = nodeOf;
    }

    /**
     * The node an origin names, for the coordinator of the querying cluster, which reaches the nodes of other clusters by
     * their cluster alias.
     */
    public static FetchNode nodeInItsCluster(DocOrigin origin) {
        return new FetchNode(origin.clusterAlias(), origin.nodeId());
    }

    /**
     * The node an origin names, for the coordinator of a remote cluster. Its documents come from its own data nodes, whose
     * origins name the alias the querying cluster gave it.
     */
    public static FetchNode nodeInThisCluster(DocOrigin origin) {
        return FetchNode.local(origin.nodeId());
    }

    @Override
    public Page pollPage() {
        Page page = delegate.pollPage();
        if (page != null) {
            try {
                record(page);
            } catch (Exception e) {
                page.releaseBlocks();
                throw e;
            }
        }
        return page;
    }

    private void record(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc) {
                DocOrigins origins = doc.asVector().origins();
                if (origins == null) {
                    continue;
                }
                for (int o = 0; o < origins.size(); o++) {
                    DocOrigin origin = origins.get(o);
                    fetchContexts.arrived(nodeOf.apply(origin), origin.contextId());
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
    public int bufferSize() {
        return delegate.bufferSize();
    }

    @Override
    public IsBlockedResult waitForReading() {
        return delegate.waitForReading();
    }
}
