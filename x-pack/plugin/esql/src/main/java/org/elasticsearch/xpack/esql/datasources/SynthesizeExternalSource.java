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
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Producer-side composition of the {@code _source} metadata column for external datasets.
 * For each row, the bound data columns ({@code <name, value>} pairs at that row's position) are
 * gathered into a {@link Map}, JSON-serialized via
 * {@link Source#fromMap(Map, XContentType)}, and emitted as a {@code BytesRef} of type
 * {@link org.elasticsearch.xpack.esql.core.type.DataType#SOURCE}.
 * <p>
 * Renders via {@link Source#fromMap} so the output matches what {@code SourceLoader}
 * produces for natively-stored indexed documents — a downstream consumer that already knows
 * how to parse {@code _source} (e.g. Kibana's row-detail view) works unchanged.
 * <p>
 * This is producer-side and per-row; callers project {@code _source} sparingly because the
 * cost is linear in the bound data column count and dominated by JSON serialization. The
 * upstream binding gates {@code _source} only on explicit {@code METADATA _source}.
 */
public final class SynthesizeExternalSource {

    /**
     * Framework-known synthetic / channel-bookkeeping column names that must never appear in the
     * rendered {@code _source}. Sourced from {@link SyntheticColumns#NAMES} so the canonical list
     * of reader-synthesized internal channels lives in one place.
     */
    static final Set<String> SYNTHETIC_COLUMN_NAMES = SyntheticColumns.NAMES;

    private SynthesizeExternalSource() {}

    /**
     * Build a {@code _source} block of length {@code positions} by composing one JSON object per
     * row from the supplied data columns. Each entry in {@code dataColumnNames} is paired
     * positionally with the matching type in {@code dataColumnTypes} and block in
     * {@code dataColumnBlocks}; columns named in {@link #SYNTHETIC_COLUMN_NAMES}
     * (framework-injected channels like {@link ColumnExtractor#ROW_POSITION_COLUMN}) are excluded.
     * A leading underscore on its own is NOT a filter — user data columns named e.g.
     * {@code _corrupt_record} or {@code _status} are legitimate data and pass through to the
     * rendered object. Values render per their declared {@link DataType}, mirroring what the
     * response layer ({@code PositionToXContent}) emits for the same columns — see
     * {@link #renderScalar}.
     */
    public static BytesRefBlock composePage(
        String[] dataColumnNames,
        DataType[] dataColumnTypes,
        Block[] dataColumnBlocks,
        int positions,
        BlockFactory factory
    ) {
        if (positions == 0) {
            return (BytesRefBlock) factory.newConstantNullBlock(0);
        }
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(positions)) {
            // TODO: per-row hot allocations (LinkedHashMap, Source.fromMap,
            // internalSourceRef().toBytesRef()) are fine at current call rate; revisit (single
            // reused map + builder) when hot.
            for (int row = 0; row < positions; row++) {
                Map<String, Object> map = new LinkedHashMap<>(dataColumnNames.length);
                for (int c = 0; c < dataColumnNames.length; c++) {
                    String name = dataColumnNames[c];
                    if (SYNTHETIC_COLUMN_NAMES.contains(name)) {
                        continue; // exclude framework-injected synthetic channels (e.g. _rowPosition)
                    }
                    Block block = dataColumnBlocks[c];
                    if (block == null) {
                        continue;
                    }
                    // BlockUtils.toJavaObject returns null for null rows, a scalar for single-value
                    // rows, and ArrayList<Object> for multi-value rows. Omitting null fields from
                    // _source matches the precedent set by SourceLoader for natively-indexed
                    // _source.
                    Object value = BlockUtils.toJavaObject(block, row);
                    if (value == null) {
                        continue;
                    }
                    map.put(name, renderValue(value, dataColumnTypes[c]));
                }
                BytesRef bytes = Source.fromMap(map, XContentType.JSON).internalSourceRef().toBytesRef();
                builder.appendBytesRef(bytes);
            }
            return builder.build();
        }
    }

    private static Object renderValue(Object value, DataType type) {
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(renderScalar(element, type));
            }
            return out;
        }
        return renderScalar(value, type);
    }

    /**
     * Render one scalar the way the response layer ({@code PositionToXContent}) renders the same
     * column type, so a value reads identically in {@code _source} and in the query output:
     * IP/VERSION decode their wire bytes ({@code utf8ToString} on those would emit garbage),
     * DATETIME/DATE_NANOS format as UTC ISO-8601 strings rather than raw epoch longs, and
     * UNSIGNED_LONG decodes the sign-flipped long into its numeric value. Types no reader can
     * emit today fail loud so a future type gets handled intentionally rather than discovered as
     * corrupt {@code _source}.
     */
    private static Object renderScalar(Object value, DataType type) {
        return switch (type) {
            case KEYWORD, TEXT -> ((BytesRef) value).utf8ToString();
            case IP -> EsqlDataTypeConverter.ipToString((BytesRef) value);
            case VERSION -> EsqlDataTypeConverter.versionToString((BytesRef) value);
            case DATETIME -> EsqlDataTypeConverter.dateTimeToString((Long) value);
            case DATE_NANOS -> EsqlDataTypeConverter.nanoTimeToString((Long) value);
            case UNSIGNED_LONG -> NumericUtils.unsignedLongAsNumber((Long) value);
            case BOOLEAN, INTEGER, LONG, DOUBLE -> value;
            default -> throw new EsqlIllegalArgumentException("cannot render _source value of type [" + type.typeName() + "]");
        };
    }
}
