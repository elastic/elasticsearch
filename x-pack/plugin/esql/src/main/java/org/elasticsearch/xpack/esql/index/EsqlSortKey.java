/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
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
     * The mapped types of the fields that a key can read: types whose {@link BlockLoader} always reads their doc values, which a
     * key needs at flush and merge, long after the index is created.
     */
    private static final Set<String> FIELD_TYPES = Set.of(
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "scaled_float",
        "unsigned_long",
        "date",
        "date_nanos",
        "boolean",
        "keyword",
        "ip",
        "version"
    );

    /**
     * Evaluating a key allocates blocks of {@link DocValuesExpressionReader#CHUNK} positions at a time, on flush and merge
     * threads that don't run under a request, so there is no breaker to account them to.
     */
    static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
        .build();

    /** Builds the blocks that the fields' {@link BlockLoader}s read, with a dictionary of the ordinals of each chunk. */
    static final BlockLoader.BlockFactory LOADER_FACTORY = new DelegatingBlockLoaderFactory(BLOCK_FACTORY) {
        @Override
        public BlockLoader.Block constantNulls(int count) {
            return factory.newConstantNullBlock(count);
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense) {
            return new ChunkOrdinals(factory, count, ordinals::lookupOrd, Block.MvOrdering.UNORDERED);
        }

        @Override
        public BlockLoader.SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
            return new ChunkOrdinals(factory, count, ordinals::lookupOrd, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.SortedSetOrdinalsBuilder arrayOrderOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
            return new ChunkOrdinals(factory, count, ordinals::lookupOrd, Block.MvOrdering.UNORDERED);
        }
    };

    /** Builds a block whose dictionary holds the distinct ordinals of the chunk, each looked up once. */
    private static final class ChunkOrdinals implements BlockLoader.SingletonOrdinalsBuilder, BlockLoader.SortedSetOrdinalsBuilder {
        private final BlockFactory factory;
        private final IntBlock.Builder ords;
        private final OrdinalLookup lookup;
        private final Block.MvOrdering mvOrdering;

        ChunkOrdinals(BlockFactory factory, int count, OrdinalLookup lookup, Block.MvOrdering mvOrdering) {
            this.factory = factory;
            this.ords = factory.newIntBlockBuilder(count);
            this.lookup = lookup;
            this.mvOrdering = mvOrdering;
        }

        @Override
        public ChunkOrdinals appendOrd(int value) {
            ords.appendInt(value);
            return this;
        }

        @Override
        public ChunkOrdinals appendOrds(int ord, int length) {
            for (int i = 0; i < length; i++) {
                ords.appendInt(ord);
            }
            return this;
        }

        @Override
        public ChunkOrdinals appendOrds(int[] values, int from, int length, int minOrd, int maxOrd) {
            for (int i = from; i < from + length; i++) {
                ords.appendInt(values[i]);
            }
            return this;
        }

        @Override
        public ChunkOrdinals appendNull() {
            ords.appendNull();
            return this;
        }

        @Override
        public ChunkOrdinals beginPositionEntry() {
            ords.beginPositionEntry();
            return this;
        }

        @Override
        public ChunkOrdinals endPositionEntry() {
            ords.endPositionEntry();
            return this;
        }

        @Override
        public BytesRefBlock build() {
            try (IntBlock ordinals = ords.build()) {
                if (ordinals.getTotalValueCount() == 0) {
                    return (BytesRefBlock) factory.newConstantNullBlock(ordinals.getPositionCount());
                }
                int[] distinct = new int[ordinals.getTotalValueCount()];
                int n = 0;
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    int first = ordinals.getFirstValueIndex(p);
                    for (int i = first; i < first + ordinals.getValueCount(p); i++) {
                        distinct[n++] = ordinals.getInt(i);
                    }
                }
                Arrays.sort(distinct);
                int size = 0;
                for (int ord : distinct) {
                    if (size == 0 || distinct[size - 1] != ord) {
                        distinct[size++] = ord;
                    }
                }
                BytesRefVector dictionary = null;
                IntBlock mapped = null;
                try {
                    try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(size)) {
                        for (int i = 0; i < size; i++) {
                            builder.appendBytesRef(lookup.lookupOrd(distinct[i]));
                        }
                        dictionary = builder.build();
                    }
                    mapped = remap(ordinals, distinct, size);
                    BytesRefBlock result = new OrdinalBytesRefBlock(mapped, dictionary);
                    dictionary = null;
                    mapped = null;
                    return result;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    Releasables.close(dictionary, mapped);
                }
            }
        }

        private IntBlock remap(IntBlock ordinals, int[] distinct, int size) {
            try (IntBlock.Builder builder = factory.newIntBlockBuilder(ordinals.getPositionCount())) {
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    int count = ordinals.getValueCount(p);
                    if (count == 0) {
                        builder.appendNull();
                        continue;
                    }
                    int first = ordinals.getFirstValueIndex(p);
                    if (count > 1) {
                        builder.beginPositionEntry();
                    }
                    for (int i = first; i < first + count; i++) {
                        builder.appendInt(Arrays.binarySearch(distinct, 0, size, ordinals.getInt(i)));
                    }
                    if (count > 1) {
                        builder.endPositionEntry();
                    }
                }
                builder.mvOrdering(mvOrdering);
                return builder.build();
            }
        }

        @Override
        public void close() {
            ords.close();
        }
    }

    private interface OrdinalLookup {
        BytesRef lookupOrd(int ord) throws IOException;
    }

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
        if (FIELD_TYPES.contains(fieldType.typeName()) == false) {
            throw error(expression, "references field [" + field + "] of unsupported type [" + fieldType.typeName() + "]");
        }
        if (fieldType.hasDocValues() == false) {
            throw error(expression, "needs doc values on field [" + field + "]");
        }
        return DataType.fromEs(fieldType.familyTypeName()).widenSmallNumeric();
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
