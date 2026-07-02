/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.xcontent.XContentString;

/**
 * {@link RoutingExtractor} for indices using the {@link IndexRouting.ExtractFromSource.ForRoutingPath}
 * strategy (LogsDB and time-series indices created before
 * {@link org.elasticsearch.index.IndexVersions#TSID_CREATED_DURING_ROUTING TSID_CREATED_DURING_ROUTING}).
 *
 * <p>Hashing parity with the source-parser-based path
 * ({@link IndexRouting.ExtractFromSource.ForRoutingPath#hashSource}) is preserved by feeding the
 * exact same UTF-8 byte slice ({@link XContentString.UTF8Bytes}) the parser already emits at every
 * leaf — see {@link RoutingHashBuilder#addHash(String, BytesRef)}. Hence
 * {@link EirfEncoder.LeafSink#passRawText()} returns {@code true}.
 */
final class RoutingPathExtractor extends RoutingExtractor {

    private final IndexRouting.ExtractFromSource.ForRoutingPath strategy;
    private final RoutingHashBuilder hashBuilder;

    RoutingPathExtractor(IndexRouting.ExtractFromSource.ForRoutingPath strategy) {
        this.strategy = strategy;
        this.hashBuilder = strategy.builder();
    }

    @Override
    public boolean passRawText() {
        return true;
    }

    @Override
    protected boolean matchesField(String dottedPath) {
        return strategy.matchesField(dottedPath);
    }

    @Override
    protected void handleTextPrimitive(String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {
        hashBuilder.addHash(dottedPath, new BytesRef(textBytes.bytes(), textBytes.offset(), textBytes.length()));
    }

    @Override
    protected void resetBuilderState() {
        hashBuilder.clear();
    }

    @Override
    public int computeShardId(IndexRequest indexRequest) {
        return strategy.shardIdForRoutingHash(hashBuilder);
    }
}
