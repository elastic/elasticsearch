/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BlockUtils.BuilderWrapper;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

/**
 * Converts a bounded {@link EqlSearchResponse} into a single {@link Page} under the typed schema of the
 * {@code EQL} source command (see {@link EqlRelation}). Kept separate from the source operator so the
 * conversion is unit-testable against a hand-built response, with no client or driver involved.
 *
 * <p>The schema is the field-caps mapping the analyzer resolved for the target indices (one typed column per
 * mapped, ES|QL-convertible field), plus, for sequence and sample queries, three prepended synthetics:
 * {@code _sequence} (long — which match a row belongs to), {@code _sequence_stage} (int — stage index within
 * the match) and {@code join_keys} (keyword — the match's join keys). Columns are emitted in schema order and
 * dispatched by attribute class, not by name; a mapped field literally named {@code _sequence}/{@code join_keys}
 * that would collide with a synthetic is rejected upstream at analysis (see {@code Analyzer.ResolveEqlRelation}).
 *
 * <p>Field values come from the EQL response's fields API ({@link Event#fetchFields()}); the request asks for
 * every convertible field (see {@link EqlRequests}). A field absent from an event, or an event that the EQL
 * engine reported as {@code missing}, yields a null in that column. Types ES|QL cannot yet extract were turned
 * into {@link UnsupportedAttribute}s at resolve time and render as all-null columns, matching {@code FROM}.
 *
 * <p>{@code METADATA} columns ({@code _index}, {@code _id}, {@code _source}) are appended last and come from the
 * event envelope ({@link Event#index()}/{@link Event#id()}/{@link Event#source()}), not the fields API.
 */
public final class EqlPageConverter {

    /**
     * The set of column types the converter can materialize from the EQL fields API. The analyzer
     * ({@code ResolveEqlRelation}) gates every mapped field against this set — anything outside it becomes an
     * {@link UnsupportedAttribute} — so {@link #norm} never sees a type without a matching arm. This is the
     * single source of truth: extending support means adding a {@code norm} arm AND a type here.
     */
    public static final Set<DataType> CONVERTIBLE_TYPES = Set.copyOf(
        EnumSet.of(KEYWORD, TEXT, LONG, INTEGER, DOUBLE, BOOLEAN, DATETIME, IP, VERSION)
    );

    private EqlPageConverter() {}

    /** One output row: an event plus, for sequence/sample matches, the match ordinal, stage index and join keys. */
    private record Row(long sequenceOrdinal, int stage, List<Object> joinKeys, Event event) {}

    static Page toPage(EqlSearchResponse response, EqlRelation.Mode mode, List<Attribute> schema, BlockFactory blockFactory) {
        List<Row> rows = mode == EqlRelation.Mode.EVENT ? eventRows(response) : sequenceRows(response);
        int positions = rows.size();
        int width = schema.size();

        BuilderWrapper[] wrappers = new BuilderWrapper[width];
        Block[] blocks = new Block[width];
        boolean success = false;
        try {
            for (int c = 0; c < width; c++) {
                Attribute attr = schema.get(c);
                // NULL-typed columns have no fields-API value — the NO_FIELDS placeholder and NULLIFY-mode unmapped
                // columns — as do unsupported columns; render them all as constant-null columns, like FROM.
                wrappers[c] = attr instanceof UnsupportedAttribute || attr.dataType() == DataType.NULL
                    ? null
                    : BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(attr.dataType()), positions);
            }
            for (Row row : rows) {
                for (int c = 0; c < width; c++) {
                    if (wrappers[c] != null) {
                        wrappers[c].accept(valueFor(schema.get(c), row));
                    }
                }
            }
            for (int c = 0; c < width; c++) {
                blocks[c] = wrappers[c] != null ? wrappers[c].builder().build() : blockFactory.newConstantNullBlock(positions);
            }
            Page page = new Page(blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(wrappers);
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private static List<Row> eventRows(EqlSearchResponse response) {
        List<Event> events = response.hits().events();
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        List<Row> rows = new ArrayList<>(events.size());
        for (Event event : events) {
            rows.add(new Row(0L, 0, null, event));
        }
        return rows;
    }

    private static List<Row> sequenceRows(EqlSearchResponse response) {
        List<Sequence> sequences = response.hits().sequences();
        if (sequences == null || sequences.isEmpty()) {
            return List.of();
        }
        List<Row> rows = new ArrayList<>();
        for (int s = 0; s < sequences.size(); s++) {
            Sequence sequence = sequences.get(s);
            List<Event> events = sequence.events();
            for (int p = 0; p < events.size(); p++) {
                rows.add(new Row(s, p, sequence.joinKeys(), events.get(p)));
            }
        }
        return rows;
    }

    /**
     * The value for one column of one row: a synthetic derived from the match, a {@code METADATA} provenance value
     * from the event envelope, or a field pulled from the event's fields API. Dispatch is by attribute class, not by
     * name; a mapped field colliding with a declared metadata column of the same name is rejected upstream at analysis.
     */
    private static Object valueFor(Attribute attr, Row row) {
        if (attr instanceof ReferenceAttribute) {
            return switch (attr.name()) {
                case EqlRelation.SEQUENCE_COLUMN -> row.sequenceOrdinal();
                case EqlRelation.SEQUENCE_STAGE_COLUMN -> row.stage();
                case EqlRelation.JOIN_KEYS_COLUMN -> joinKeysValue(row.joinKeys());
                default -> throw new EsqlIllegalArgumentException("unexpected EQL synthetic column [{}]", attr.name());
            };
        }
        if (attr instanceof MetadataAttribute) {
            return metadataValue(attr.name(), row.event());
        }
        if (attr instanceof FieldAttribute fa) {
            return fieldValue(fa, row.event());
        }
        throw new EsqlIllegalArgumentException("unexpected EQL column [{}] of type [{}]", attr.name(), attr.getClass().getName());
    }

    /**
     * The value of a {@code METADATA} column for one event, from the response envelope. {@code null} for a missing
     * event (checked before the accessors so {@code MISSING_EVENT}'s empty index/id/source do not leak).
     */
    private static Object metadataValue(String name, Event event) {
        if (event == null || event.missing()) {
            return null;
        }
        if (MetadataAttribute.INDEX.equals(name)) {
            return event.index();
        }
        if (IdFieldMapper.NAME.equals(name)) {
            return event.id();
        }
        if (SourceFieldMapper.NAME.equals(name)) {
            BytesReference source = event.source();
            // appendBytesRef copies into the block, so we do not retain the ref-counted response's bytes.
            return source == null ? null : source.toBytesRef();
        }
        throw new EsqlIllegalArgumentException("unexpected EQL metadata column [{}]", name);
    }

    /** Join keys as a single keyword value: {@code null} when empty, one value, or a multivalue entry. Nulls are dropped. */
    private static Object joinKeysValue(List<Object> joinKeys) {
        if (joinKeys == null || joinKeys.isEmpty()) {
            return null;
        }
        List<Object> out = new ArrayList<>(joinKeys.size());
        for (Object key : joinKeys) {
            if (key != null) {
                out.add(String.valueOf(key));
            }
        }
        if (out.isEmpty()) {
            return null;
        }
        return out.size() == 1 ? out.get(0) : out;
    }

    /** The typed value of one field for one event: {@code null} for a missing event or an absent/empty field. */
    private static Object fieldValue(FieldAttribute fa, Event event) {
        if (event == null || event.missing()) {
            return null;
        }
        Map<String, DocumentField> fetched = event.fetchFields();
        if (fetched == null) {
            return null;
        }
        DocumentField field = fetched.get(fa.fieldName().string());
        if (field == null) {
            return null;
        }
        List<Object> values = field.getValues();
        if (values == null || values.isEmpty()) {
            return null;
        }
        DataType type = fa.dataType();
        if (values.size() == 1) {
            Object value = values.get(0);
            return value == null ? null : norm(value, type);
        }
        List<Object> out = new ArrayList<>(values.size());
        for (Object value : values) {
            // Drop null elements — norm never returns null, and a multivalue position cannot hold a null.
            if (value != null) {
                out.add(norm(value, type));
            }
        }
        return out.isEmpty() ? null : out;
    }

    /**
     * Normalizes one fields-API value to the exact Java type {@link BlockUtils#appendValue} expects for the
     * column's {@link org.elasticsearch.compute.data.ElementType}. The fields API renders values as whatever the
     * fetch phase produced (numbers can arrive boxed as {@code Integer} where {@code Long} is expected, dates as
     * epoch-millis strings), so every arm tolerates both {@link Number} and {@link String} rather than blind-casting.
     * The {@code default} arm is an unreachable tripwire — the resolve-time gate ({@link #CONVERTIBLE_TYPES}) turns
     * any other type into an {@link UnsupportedAttribute} before it can reach here.
     */
    private static Object norm(Object value, DataType type) {
        return switch (type) {
            case KEYWORD, TEXT -> value.toString();
            case LONG, DATETIME -> value instanceof Number n ? n.longValue() : Long.parseLong(value.toString());
            case INTEGER -> value instanceof Number n ? n.intValue() : Integer.parseInt(value.toString());
            case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString());
            case BOOLEAN -> value instanceof Boolean b ? b : Booleans.parseBoolean(value.toString());
            case IP -> EsqlDataTypeConverter.stringToIP(value.toString());
            case VERSION -> EsqlDataTypeConverter.stringToVersion(value.toString());
            default -> throw new EsqlIllegalArgumentException("EQL command cannot convert [{}] value", type.typeName());
        };
    }
}
