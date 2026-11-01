/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds ES|QL lookup transport requests from outside the ES|QL plugin, so that tests can send requests whose
 * {@code indexPattern} and {@code shardId} were chosen independently.
 */
public final class LookupRequestFactory {

    public static final String ACTION_NAME = LookupFromIndexService.LOOKUP_ACTION_NAME;
    public static final String ENRICH_ACTION_NAME = EnrichLookupService.LOOKUP_ACTION_NAME;

    private static final BlockFactory BLOCK_FACTORY = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);

    private LookupRequestFactory() {}

    /**
     * A {@code LOOKUP JOIN ... ON matchField} request for a single keyword {@code key}, returning the keyword {@code extractField}
     * of the matching documents in the index of {@code shardId}.
     */
    public static Writeable keywordLookup(String indexPattern, ShardId shardId, String matchField, String key, String extractField) {
        return new LookupFromIndexService.LookupRequest(
            "session",
            shardId,
            indexPattern,
            new Page(BLOCK_FACTORY.newConstantBytesRefBlockWith(new BytesRef(key), 1)),
            null,
            List.of(
                new FieldAttribute(
                    Source.EMPTY,
                    extractField,
                    new EsField(extractField, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                )
            ),
            List.of(new MatchConfig(matchField, 0, DataType.KEYWORD)),
            Source.EMPTY,
            null,
            null,
            null,
            null,
            false,
            null
        );
    }

    /**
     * An {@code ENRICH ... ON matchField} request with a {@code match} policy for a single keyword {@code key}, returning the keyword
     * {@code extractField} of the matching documents in the index of {@code shardId}. Its index pattern is always the name of the
     * index in {@code shardId}.
     */
    public static Writeable keywordEnrich(ShardId shardId, String matchField, String key, String extractField) {
        return new EnrichLookupService.TransportRequest(
            "session",
            shardId,
            DataType.KEYWORD,
            "match",
            matchField,
            new Page(BLOCK_FACTORY.newConstantBytesRefBlockWith(new BytesRef(key), 1)),
            null,
            List.of(
                new FieldAttribute(
                    Source.EMPTY,
                    extractField,
                    new EsField(extractField, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                )
            ),
            Source.EMPTY
        );
    }

    /**
     * The keyword values returned by a lookup request.
     */
    public static final class KeywordValues extends ActionResponse {
        private final List<String> values;

        public KeywordValues(StreamInput in) throws IOException {
            this(new LookupFromIndexService.LookupResponse(in, BLOCK_FACTORY, new ThreadContext(Settings.EMPTY)));
        }

        public static KeywordValues readEnrichResponse(StreamInput in) throws IOException {
            return new KeywordValues(new EnrichLookupService.LookupResponse(in, BLOCK_FACTORY, new ThreadContext(Settings.EMPTY)));
        }

        private KeywordValues(AbstractLookupService.LookupResponse response) {
            List<Page> pages = response.takePages();
            try {
                values = new ArrayList<>();
                for (Page page : pages) {
                    for (int b = 0; b < page.getBlockCount(); b++) {
                        Block block = page.getBlock(b);
                        if (block instanceof BytesRefBlock bytesRefBlock) {
                            for (int i = 0; i < bytesRefBlock.getTotalValueCount(); i++) {
                                values.add(bytesRefBlock.getBytesRef(i, new BytesRef()).utf8ToString());
                            }
                        }
                    }
                }
            } finally {
                Releasables.close(pages);
                response.decRef();
            }
        }

        public List<String> values() {
            return values;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException();
        }
    }
}
