/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.xcontent.XContentString;

/**
 * {@link RoutingExtractor} for indices using the
 * {@link IndexRouting.ExtractFromSource.ForIndexDimensions} strategy (time-series indices created
 * with {@link org.elasticsearch.index.IndexVersions#TSID_CREATED_DURING_ROUTING TSID_CREATED_DURING_ROUTING}
 * or later, which build the tsid during routing and attach it to the index request so the data node
 * can reuse it).
 *
 * <p>Tsid parity with the source-parser-based path
 * ({@link IndexRouting.ExtractFromSource.ForIndexDimensions#buildTsid(org.elasticsearch.xcontent.XContentType,
 * org.elasticsearch.common.bytes.BytesReference) ForIndexDimensions.buildTsid}, which feeds
 * {@link XContentParserTsidFunnel}) is preserved by mapping each {@link EirfType} the encoder
 * produces to the same {@link TsidBuilder} call the funnel would have made for the same parser
 * token. The extractor returns {@code false} from {@link EirfEncoder.LeafSink#passRawText()} so
 * the encoder hands it unboxed values directly, avoiding a wasteful
 * {@code parser.optimizedText().bytes()} call per numeric / boolean leaf.
 *
 * <p>The encoder narrows numeric values by range — values that fit in an int land at
 * {@code EirfType.INT} regardless of how Jackson reported {@code numberType()}; values that don't
 * land at {@code LONG}. That matches Jackson's own {@code numberType()} discrimination (which the
 * funnel uses) because Jackson reports the smallest integral type that fits the value, so the
 * resulting tsid bytes are identical. Floating-point values that the encoder narrowed from
 * {@code DOUBLE} to {@code FLOAT} are guaranteed (by the encoder's narrowing condition
 * {@code (double)(float) val == val}) to be losslessly recoverable, so the {@code double} the
 * encoder hands us in {@link #handleDoublePrimitive} matches what the funnel would have supplied.
 */
final class DimensionsExtractor extends RoutingExtractor {

    private final IndexRouting.ExtractFromSource.ForIndexDimensions strategy;
    private final TsidBuilder tsidBuilder;

    DimensionsExtractor(IndexRouting.ExtractFromSource.ForIndexDimensions strategy) {
        this.strategy = strategy;
        this.tsidBuilder = new TsidBuilder();
    }

    @Override
    protected void resetBuilderState() {
        tsidBuilder.reset();
    }

    @Override
    public boolean passRawText() {
        return false;
    }

    @Override
    protected boolean matchesField(String dottedPath) {
        return strategy.matchesField(dottedPath);
    }

    @Override
    protected void handleTextPrimitive(String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {
        // The encoder funnels every text-typed leaf (including BIG_INTEGER / BIG_DECIMAL) through
        // here as EirfType.STRING; anything else would indicate an encoder bug.
        assert type == EirfType.STRING : "expected STRING, got type=" + type;
        tsidBuilder.addStringDimension(dottedPath, textBytes);
    }

    @Override
    protected void handleLongPrimitive(String dottedPath, byte type, long value) {
        if (type == EirfType.INT) {
            tsidBuilder.addIntDimension(dottedPath, (int) value);
        } else {
            // EirfType.LONG (the only other type the encoder can hand us in this method).
            tsidBuilder.addLongDimension(dottedPath, value);
        }
    }

    @Override
    protected void handleDoublePrimitive(String dottedPath, byte type, double value) {
        tsidBuilder.addDoubleDimension(dottedPath, value);
    }

    @Override
    protected void handleBooleanPrimitive(String dottedPath, boolean value) {
        tsidBuilder.addBooleanDimension(dottedPath, value);
    }

    @Override
    public int computeShardId(IndexRequest indexRequest) {
        return strategy.shardIdForExtractedTsid(tsidBuilder, indexRequest);
    }
}
