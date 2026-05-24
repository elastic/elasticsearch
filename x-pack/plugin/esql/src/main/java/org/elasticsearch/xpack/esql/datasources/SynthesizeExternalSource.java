/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Producer-side composition of the {@code _source} metadata column for external datasets.
 * For each row, the bound data columns ({@code <name, value>} pairs at that row's position) are
 * gathered into a {@link Map}, JSON-serialized via
 * {@link Source#fromMap(Map, XContentType)}, and emitted as a {@code BytesRef} of type
 * {@link org.elasticsearch.xpack.esql.core.type.DataType#SOURCE}.
 * <p>
 * Reuses the framework's source synthesis path so the rendered bytes match what
 * {@code SourceLoader} produces for natively-stored indexed documents — a downstream
 * consumer that already knows how to parse {@code _source} (e.g. Kibana's row-detail view)
 * works unchanged.
 * <p>
 * This is producer-side and per-row; callers project {@code _source} sparingly because the
 * cost is linear in the bound data column count and dominated by JSON serialization. The
 * upstream binding gates {@code _source} only on explicit {@code METADATA _source}.
 */
public final class SynthesizeExternalSource {

    /**
     * Framework-known synthetic / channel-bookkeeping column names that must never appear in the
     * rendered {@code _source}. Restricted to columns the producer pipeline injects itself
     * (currently only {@link ColumnExtractor#ROW_POSITION_COLUMN}). User data columns whose names
     * happen to start with {@code _} (e.g. Spark's {@code _corrupt_record}, a user-supplied
     * {@code _status}) are real data and pass through to the rendered object.
     */
    static final Set<String> SYNTHETIC_COLUMN_NAMES = Set.of(ColumnExtractor.ROW_POSITION_COLUMN);

    private SynthesizeExternalSource() {}

    /**
     * Build a {@code _source} block of length {@code positions} by composing one JSON object per
     * row from the supplied data columns. Each entry in {@code dataColumnNames} is paired
     * positionally with the matching block in {@code dataColumnBlocks}; columns named in
     * {@link #SYNTHETIC_COLUMN_NAMES} (framework-injected channels like
     * {@link ColumnExtractor#ROW_POSITION_COLUMN}) are excluded. A leading underscore on its own
     * is NOT a filter — user data columns named e.g. {@code _corrupt_record} or {@code _status}
     * are legitimate data and pass through to the rendered object.
     */
    public static BytesRefBlock composePage(String[] dataColumnNames, Block[] dataColumnBlocks, int positions, BlockFactory factory) {
        if (positions == 0) {
            return (BytesRefBlock) factory.newConstantNullBlock(0);
        }
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(positions)) {
            BytesRef scratch = new BytesRef();
            for (int row = 0; row < positions; row++) {
                Map<String, Object> map = new LinkedHashMap<>(dataColumnNames.length);
                for (int c = 0; c < dataColumnNames.length; c++) {
                    String name = dataColumnNames[c];
                    if (SYNTHETIC_COLUMN_NAMES.contains(name)) {
                        continue; // exclude framework-injected synthetic channels (e.g. _rowPosition)
                    }
                    Block block = dataColumnBlocks[c];
                    if (block == null || block.isNull(row)) {
                        // Omit null fields from _source rather than write JSON null, matching
                        // the precedent set by SourceLoader for natively-indexed _source.
                        continue;
                    }
                    map.put(name, valueAt(block, row, scratch));
                }
                BytesRef bytes = Source.fromMap(map, XContentType.JSON).internalSourceRef().toBytesRef();
                builder.appendBytesRef(bytes);
            }
            return builder.build();
        }
    }

    private static Object valueAt(Block block, int row, BytesRef scratch) {
        int valueIdx = block.getFirstValueIndex(row);
        return switch (block) {
            case LongBlock l -> l.getLong(valueIdx);
            case IntBlock i -> i.getInt(valueIdx);
            case DoubleBlock d -> d.getDouble(valueIdx);
            case BooleanBlock b -> b.getBoolean(valueIdx);
            case BytesRefBlock br -> {
                BytesRef out = br.getBytesRef(valueIdx, scratch);
                // Render BytesRef as String — fromMap will JSON-encode it.
                yield out.utf8ToString();
            }
            // Default arm intentional: any new Block subtype falls through to its toString().
            // Not asserting because EsqlBlockTypes can introduce new types and we prefer a
            // best-effort rendering over a hard failure on a producer-thread.
            default -> block.toString();
        };
    }
}
