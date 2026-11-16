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

    public ArrivingDocumentsSource(ExchangeSource delegate, FetchContextLease.Execution fetchContexts) {
        this.delegate = delegate;
        this.fetchContexts = fetchContexts;
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
                    fetchContexts.arrived(new FetchNode(origin.clusterAlias(), origin.nodeId()), origin.contextId());
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
