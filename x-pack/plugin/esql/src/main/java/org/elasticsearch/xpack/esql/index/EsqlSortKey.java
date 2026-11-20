/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.FieldExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * An index sort key computed by an ES|QL expression from the doc values of one or more fields, such as
 * {@code DATE_TRUNC(1 hour, @timestamp)}. The key is evaluated whenever Lucene sorts a segment, so it is never stored.
 * <p>
 * Segments sorted by one node are merged by another, so the key is a {@link FieldExpression}, which computes the same values
 * wherever and whenever it runs.
 */
public final class EsqlSortKey {

    /** Types of key that sort as longs, see {@link EsqlSortKeyValues}. */
    private static final Set<DataType> KEY_TYPES = Set.of(
        DataType.LONG,
        DataType.INTEGER,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.DATE_NANOS
    );

    /**
     * Evaluating a key allocates blocks of {@link DocValuesExpressionReader#CHUNK} positions at a time, on flush and merge
     * threads that don't run under a request, so there is no breaker to account them to.
     */
    static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
        .build();

    /** Builds the blocks that the fields' {@link BlockLoader}s read. */
    static final BlockLoader.BlockFactory LOADER_FACTORY = new DelegatingBlockLoaderFactory(BLOCK_FACTORY) {
        @Override
        public BlockLoader.Block constantNulls(int count) {
            return factory.newConstantNullBlock(count);
        }
    };

    private final String expression;
    private final List<BlockLoader> loaders;
    private final DataType type;
    private final ExpressionEvaluator.Factory evaluator;

    private EsqlSortKey(String expression, List<BlockLoader> loaders, DataType type, ExpressionEvaluator.Factory evaluator) {
        this.expression = expression;
        this.loaders = loaders;
        this.type = type;
        this.evaluator = evaluator;
    }

    /**
     * Checks a parsed sort key without the mapping, see {@link FieldExpression#fields}.
     *
     * @return the fields the key reads
     */
    static Set<String> validate(String expression, Expression key) {
        try {
            return FieldExpression.fields(key);
        } catch (IllegalArgumentException e) {
            throw error(expression, e.getMessage(), e);
        }
    }

    /**
     * Parses and resolves a sort key against the mapping, and builds the {@link BlockLoader}s of the fields it reads.
     *
     * @param fieldTypes the mapped type of each field, or {@code null} for an unknown field
     */
    public static EsqlSortKey resolve(String expression, IndexSettings indexSettings, Function<String, MappedFieldType> fieldTypes) {
        Expression key = parse(expression);
        Map<String, DataType> types = new LinkedHashMap<>();
        List<BlockLoader> loaders = new ArrayList<>();
        for (String field : validate(expression, key)) {
            MappedFieldType fieldType = fieldTypes.apply(field);
            types.put(field, dataType(expression, field, fieldType));
            loaders.add(blockLoader(expression, field, fieldType, indexSettings));
        }
        FieldExpression compiled;
        try {
            compiled = FieldExpression.compile(key, types);
        } catch (IllegalArgumentException e) {
            throw error(expression, e.getMessage(), e);
        }
        if (KEY_TYPES.contains(compiled.type()) == false) {
            throw error(expression, "returns type [" + compiled.type().typeName() + "], which isn't supported yet");
        }
        return new EsqlSortKey(expression, List.copyOf(loaders), compiled.type(), compiled.evaluator());
    }

    /** The ES|QL type of a field the key reads from doc values. */
    private static DataType dataType(String expression, String field, MappedFieldType fieldType) {
        if (fieldType == null) {
            throw error(expression, "references unknown field [" + field + "]");
        }
        if (fieldType.name().equals(field) == false) {
            throw error(expression, "can't use alias [" + field + "]");
        }
        if (fieldType instanceof AbstractScriptFieldType<?>) {
            throw error(expression, "can't use runtime field [" + field + "]");
        }
        if (fieldType.hasDocValues() == false) {
            throw error(expression, "needs doc values on field [" + field + "]");
        }
        DataType type = DataType.fromEs(fieldType.familyTypeName());
        // Spatial doc values are encoded differently from the values that ES|QL functions take
        if (type == null || type == DataType.UNSUPPORTED || DataType.isSpatial(type)) {
            throw error(expression, "references field [" + field + "] of unsupported type [" + fieldType.typeName() + "]");
        }
        return type.widenSmallNumeric();
    }

    private static BlockLoader blockLoader(String expression, String field, MappedFieldType fieldType, IndexSettings indexSettings) {
        BlockLoader loader;
        try {
            loader = fieldType.blockLoader(new DocValuesLoaderContext(indexSettings));
        } catch (UnsupportedOperationException e) {
            loader = null;
        }
        if (loader == null) {
            throw error(expression, "can't read field [" + field + "] of type [" + fieldType.typeName() + "] from doc values");
        }
        return loader;
    }

    /** Parses an {@code index.sort} expression as the keys of an ES|QL {@code SORT}. */
    static List<Order> parseSort(String expression) {
        LogicalPlan plan;
        try {
            plan = FieldExpression.parser().parseQuery("FROM i | SORT " + expression);
        } catch (ParsingException e) {
            throw sortError(expression, "can't be parsed: " + e.getErrorMessage());
        }
        // Anything else, for example a pipe to another command, isn't a sort
        if (plan instanceof OrderBy orderBy && orderBy.child() instanceof UnresolvedRelation) {
            return orderBy.order();
        }
        throw sortError(expression, "is not a sort");
    }

    private static IllegalArgumentException sortError(String expression, String message) {
        return new IllegalArgumentException(
            "setting [" + IndexSortConfig.INDEX_SORT_SETTING.getKey() + "] " + message + " in [" + expression + "]"
        );
    }

    /** Parses a key, as found by {@link #parseSort}: in parentheses, it can't have an order or be followed by another key. */
    private static Expression parse(String expression) {
        List<Order> orders;
        try {
            orders = parseSort("(" + expression + ")");
        } catch (IllegalArgumentException e) {
            throw error(expression, "is not a single sort key", e);
        }
        return orders.getFirst().child();
    }

    private static IllegalArgumentException error(String expression, String message) {
        return error(expression, message, null);
    }

    private static IllegalArgumentException error(String expression, String message, Exception cause) {
        return new IllegalArgumentException("index sort key [" + expression + "] " + message, cause);
    }

    public String expression() {
        return expression;
    }

    /** The type of the key, one that sorts as longs, see {@link EsqlSortKeyValues}. */
    public DataType type() {
        return type;
    }

    /**
     * The keys of a segment's documents. Documents for which the expression returns {@code null} or several values take
     * {@code missing}.
     */
    EsqlSortKeyValues values(LeafReader reader, int maxDoc, long missing) throws IOException {
        DriverContext context = new DriverContext(
            BigArrays.NON_RECYCLING_INSTANCE,
            BLOCK_FACTORY,
            null,
            "index sort",
            DriverContext.WarningsMode.IGNORE
        );
        var expressionReader = new DocValuesExpressionReader(reader.getContext(), maxDoc, loaders, evaluator.get(context), LOADER_FACTORY);
        return new EsqlSortKeyValues(expressionReader, type, missing);
    }

    @Override
    public String toString() {
        return expression;
    }

    /** Loads fields from doc values only, the only data a segment has when Lucene sorts it. */
    private record DocValuesLoaderContext(IndexSettings indexSettings) implements MappedFieldType.BlockLoaderContext {
        @Override
        public String indexName() {
            return indexSettings.getIndex().getName();
        }

        @Override
        public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
            return MappedFieldType.FieldExtractPreference.DOC_VALUES;
        }

        @Override
        public SearchLookup lookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> sourcePaths(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String parentField(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappingLookup mappingLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return null;
        }

        @Override
        public Warnings warnings() {
            return null;
        }

        @Override
        public ByteSizeValue ordinalsByteSize() {
            return DEFAULT_ORDINALS_BYTE_SIZE;
        }

        @Override
        public ByteSizeValue scriptByteSize() {
            return DEFAULT_SCRIPT_BYTE_SIZE;
        }
    }
}
