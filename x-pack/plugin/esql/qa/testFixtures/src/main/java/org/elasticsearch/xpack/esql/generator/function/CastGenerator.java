/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * Generates casts of a column that are expected to succeed, so the generated queries reach the real cast behavior rather
 * than an analysis error:
 * <ul>
 *     <li>A strict cast ({@code f::type} or {@code TO_TYPE(f)}) only when every type behind the column converts.</li>
 *     <li>A lenient cast ({@code f::~type}) in that case too, and also on a union where at least one type converts. That
 *     second case loads the other indices as null.</li>
 * </ul>
 * Which source types a target accepts comes from the conversion functions themselves.
 */
public final class CastGenerator {

    private static final Set<DataType> EXCLUDED_TARGETS = Set.of(
        // A converted geotile can be an invalid zoom/x/y, and writing it to the response kills the node:
        // https://github.com/elastic/elasticsearch/issues/160230
        DataType.GEOTILE
    );

    /**
     * Cast target to the source types its conversion accepts, e.g. {@code LONG -> [KEYWORD, INTEGER, DATETIME, ...]}.
     * The targets are every type {@code ::} can cast a field to, except {@link #EXCLUDED_TARGETS}, in a fixed order so a test
     * seed always generates the same cast.
     */
    private static final Map<DataType, Set<DataType>> SUPPORTED_SOURCE_TYPES;
    /**
     * Cast target to the name of its conversion function, e.g. {@code LONG -> "to_long"}.
     */
    private static final Map<DataType, String> FUNCTION_NAMES;

    static {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry().snapshotRegistry();
        Map<DataType, Set<DataType>> supported = new LinkedHashMap<>();
        Map<DataType, String> names = new LinkedHashMap<>();
        for (DataType target : DataType.types()) {
            var factory = EsqlDataTypeConverter.converterFunctionFactory(target);
            if (factory == null || EXCLUDED_TARGETS.contains(target) || target.supportedVersion().supportedLocally() == false) {
                continue;
            }
            AbstractConvertFunction convert = factory.apply(Source.EMPTY, Literal.NULL, ConfigurationAware.CONFIGURATION_MARKER);
            // TO_DATE_PERIOD and TO_TIME_DURATION only accept constants, never a field.
            if (convert instanceof FoldablesConvertFunction) {
                continue;
            }
            // Snapshot-only conversions are not registered in release builds.
            if (Build.current().isSnapshot() == false && EsqlFunctionRegistry.isSnapshotOnly(convert.getClass())) {
                continue;
            }
            supported.put(target, convert.supportedTypes());
            names.put(target, registry.functionName(convert.getClass()).toLowerCase(Locale.ROOT));
        }
        SUPPORTED_SOURCE_TYPES = Collections.unmodifiableMap(supported);
        FUNCTION_NAMES = Map.copyOf(names);
    }

    /**
     * A generated cast.
     * @param expression the ES|QL expression
     * @param target the type it casts to
     * @param lenient whether it uses the lenient cast operator
     * @param sourceTypes the types behind the cast column
     */
    public record Cast(String expression, DataType target, boolean lenient, List<DataType> sourceTypes) {}

    private CastGenerator() {}

    /**
     * A random cast of one of the columns, or {@code null} when no column can be cast.
     * @param allowLenient whether lenient casts may be generated
     */
    public static Cast randomCast(List<Column> columns, boolean allowLenient) {
        List<Cast> candidates = new ArrayList<>();
        for (Column column : columns) {
            List<DataType> sources = sourceTypes(column);
            if (sources.isEmpty()) {
                continue;
            }
            String name = EsqlQueryGenerator.needsQuoting(column.name()) ? EsqlQueryGenerator.quote(column.name()) : column.name();
            for (DataType target : SUPPORTED_SOURCE_TYPES.keySet()) {
                long converting = sources.stream().filter(source -> converts(target, source)).count();
                if (converting == sources.size()) {
                    String strict = randomBoolean() ? name + "::" + target.typeName() : FUNCTION_NAMES.get(target) + "(" + name + ")";
                    candidates.add(new Cast(strict, target, false, sources));
                }
                if (allowLenient && converting > 0) {
                    candidates.add(new Cast(name + "::~" + target.typeName(), target, true, sources));
                }
            }
        }
        return candidates.isEmpty() ? null : randomFrom(candidates);
    }

    /**
     * Whether a {@code target} cast accepts a {@code source} value.
     */
    public static boolean converts(DataType target, DataType source) {
        return SUPPORTED_SOURCE_TYPES.get(target).contains(source.widenSmallNumeric());
    }

    /**
     * The types behind a column: its original types for a union, otherwise its type. Empty when the column cannot be cast:
     * an unsupported type without original types, a {@code null} column, or a union with an unsupported branch, which fails
     * before any cast applies.
     */
    static List<DataType> sourceTypes(Column column) {
        if (column.type() == null) {
            return List.of();
        }
        List<String> names = "unsupported".equals(column.type())
            && column.originalTypes() != null
            && column.originalTypes().isEmpty() == false ? column.originalTypes() : List.of(column.type());
        List<DataType> types = new ArrayList<>(names.size());
        for (String name : names) {
            DataType type = DataType.fromTypeName(name);
            if (type == null || type == DataType.UNSUPPORTED || type == DataType.NULL) {
                return List.of();
            }
            types.add(type);
        }
        return types;
    }
}
