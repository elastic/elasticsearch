/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Generates a list of Lucene queries based on the input block.
 */
public abstract class QueryList {
    protected final SearchExecutionContext searchExecutionContext;
    protected final AliasFilter aliasFilter;
    protected final MappedFieldType field;
    protected final Block block;
    @Nullable
    protected final OnlySingleValueParams onlySingleValueParams;

    protected QueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        Block block,
        OnlySingleValueParams onlySingleValueParams
    ) {
        this.searchExecutionContext = searchExecutionContext;
        this.aliasFilter = aliasFilter;
        this.field = field;
        this.block = block;
        this.onlySingleValueParams = onlySingleValueParams;
    }

    /**
     * Returns the number of positions in this query list
     */
    int getPositionCount() {
        return block.getPositionCount();
    }

    /**
     * Returns a copy of this query list that only returns queries for single-valued positions.
     * That is, it returns `null` queries for either multivalued or null positions.
     * <p>
     * Whenever a multi-value position is encountered, whether in the input block or in the queried index, a warning is emitted.
     * </p>
     */
    public abstract QueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage);

    final Query getQuery(int position) {
        final int valueCount = block.getValueCount(position);
        if (onlySingleValueParams != null && valueCount != 1) {
            if (valueCount > 1) {
                onlySingleValueParams.warnings.registerException(
                    new IllegalArgumentException(onlySingleValueParams.multiValueWarningMessage)
                );
            }
            return null;
        }
        final int firstValueIndex = block.getFirstValueIndex(position);

        Query query = doGetQuery(position, firstValueIndex, valueCount);

        if (aliasFilter != null && aliasFilter != AliasFilter.EMPTY) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, BooleanClause.Occur.FILTER);
            try {
                builder.add(aliasFilter.getQueryBuilder().toQuery(searchExecutionContext), BooleanClause.Occur.FILTER);
                query = builder.build();
            } catch (IOException e) {
                throw new UncheckedIOException("Error while building query for alias filter", e);
            }
        }

        if (onlySingleValueParams != null) {
            query = wrapSingleValueQuery(query);
        }

        return query;
    }

    /**
     * Returns the query at the given position.
     */
    @Nullable
    abstract Query doGetQuery(int position, int firstValueIndex, int valueCount);

    private Query wrapSingleValueQuery(Query query) {
        assert onlySingleValueParams != null : "Requested to wrap single value query without single value params";

        SingleValueMatchQuery singleValueQuery = new SingleValueMatchQuery(
            searchExecutionContext.getForField(field, MappedFieldType.FielddataOperation.SEARCH),
            // Not emitting warnings for multivalued fields not matching
            onlySingleValueParams.warnings,
            onlySingleValueParams.multiValueWarningMessage
        );

        Query rewrite;
        try {
            rewrite = singleValueQuery.rewrite(searchExecutionContext.searcher());
            if (rewrite instanceof MatchAllDocsQuery) {
                // nothing to filter
                return query;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error while rewriting SingleValueQuery", e);
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(query, BooleanClause.Occur.FILTER);
        builder.add(rewrite, BooleanClause.Occur.FILTER);
        return builder.build();
    }

    /**
     * Returns a list of term queries for the given field and the input block
     * using only the {@link ElementType} of the {@link Block} to determine the
     * query.
     */
    public static QueryList rawTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        Block block
    ) {
        IntFunction<Object> blockToJavaObject = switch (block.elementType()) {
            case BOOLEAN -> {
                BooleanBlock booleanBlock = (BooleanBlock) block;
                yield booleanBlock::getBoolean;
            }
            case BYTES_REF -> offset -> {
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
                return bytesRefBlock.getBytesRef(offset, new BytesRef());
            };
            case DOUBLE -> {
                DoubleBlock doubleBlock = ((DoubleBlock) block);
                yield doubleBlock::getDouble;
            }
            case FLOAT -> {
                FloatBlock floatBlock = ((FloatBlock) block);
                yield floatBlock::getFloat;
            }
            case LONG -> {
                LongBlock intBlock = (LongBlock) block;
                yield intBlock::getLong;
            }
            case INT -> {
                IntBlock intBlock = (IntBlock) block;
                yield intBlock::getInt;
            }
            case NULL -> offset -> null;
            case DOC -> throw new IllegalArgumentException("can't read values from [doc] block");
            case COMPOSITE -> throw new IllegalArgumentException("can't read values from [composite] block");
            case AGGREGATE_METRIC_DOUBLE -> throw new IllegalArgumentException("can't read values from [aggregate metric double] block");
            case UNKNOWN -> throw new IllegalArgumentException("can't read values from [" + block + "]");
        };
        return new TermQueryList(field, searchExecutionContext, aliasFilter, block, null, blockToJavaObject);
    }

    /**
     * Returns a list of term queries for the given field and the input block of
     * {@code ip} field values.
     */
    public static QueryList ipTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        BytesRefBlock block
    ) {
        BytesRef scratch = new BytesRef();
        byte[] ipBytes = new byte[InetAddressPoint.BYTES];
        return new TermQueryList(field, searchExecutionContext, aliasFilter, block, null, offset -> {
            final var bytes = block.getBytesRef(offset, scratch);
            if (ipBytes.length != bytes.length) {
                // Lucene only support 16-byte IP addresses, even IPv4 is encoded in 16 bytes
                throw new IllegalStateException("Cannot decode IP field from bytes of length " + bytes.length);
            }
            System.arraycopy(bytes.bytes, bytes.offset, ipBytes, 0, bytes.length);
            return InetAddressPoint.decode(ipBytes);
        });
    }

    /**
     * Returns a list of term queries for the given field and the input block of
     * {@code date} field values.
     */
    public static QueryList dateTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        LongBlock block
    ) {
        return new TermQueryList(
            field,
            searchExecutionContext,
            aliasFilter,
            block,
            null,
            field instanceof RangeFieldMapper.RangeFieldType rangeFieldType
                ? offset -> rangeFieldType.dateTimeFormatter().formatMillis(block.getLong(offset))
                : block::getLong
        );
    }

    /**
     * Returns a list of term queries for the given field and the input block of
     * {@code date_nanos} field values.
     */
    public static QueryList dateNanosTermQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, LongBlock block) {
        return new DateNanosQueryList(field, searchExecutionContext, block, null);
    }

    /**
     * Returns a list of geo_shape queries for the given field and the input block.
     */
    public static QueryList geoShapeQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        Block block
    ) {
        return new GeoShapeQueryList(field, searchExecutionContext, aliasFilter, block, null);
    }

    private static class TermQueryList extends QueryList {
        private final IntFunction<Object> blockValueReader;

        private TermQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            AliasFilter aliasFilter,
            Block block,
            OnlySingleValueParams onlySingleValueParams,
            IntFunction<Object> blockValueReader
        ) {
            super(field, searchExecutionContext, aliasFilter, block, onlySingleValueParams);
            this.blockValueReader = blockValueReader;
        }

        @Override
        public TermQueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
            return new TermQueryList(
                field,
                searchExecutionContext,
                aliasFilter,
                block,
                new OnlySingleValueParams(warnings, multiValueWarningMessage),
                blockValueReader
            );
        }

        @Override
        Query doGetQuery(int position, int firstValueIndex, int valueCount) {
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> field.termQuery(blockValueReader.apply(firstValueIndex), searchExecutionContext);
                default -> {
                    final List<Object> terms = new ArrayList<>(valueCount);
                    for (int i = 0; i < valueCount; i++) {
                        final Object value = blockValueReader.apply(firstValueIndex + i);
                        terms.add(value);
                    }
                    yield field.termsQuery(terms, searchExecutionContext);
                }
            };
        }
    }

    private static class DateNanosQueryList extends QueryList {
        protected final IntFunction<Long> blockValueReader;
        private final DateFieldMapper.DateFieldType dateFieldType;

        private DateNanosQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            LongBlock block,
            OnlySingleValueParams onlySingleValueParams
        ) {
            super(field, searchExecutionContext, block, onlySingleValueParams);
            if (field instanceof RangeFieldMapper.RangeFieldType rangeFieldType) {
                // TODO: do this validation earlier
                throw new IllegalArgumentException(
                    "DateNanosQueryList does not support range fields [" + rangeFieldType + "]: " + field.name()
                );
            }
            this.blockValueReader = block::getLong;
            if (field instanceof DateFieldMapper.DateFieldType dateFieldType) {
                // Validate that the field is a date_nanos field
                // TODO: Consider allowing date_nanos to match normal datetime fields
                if (dateFieldType.resolution() != DateFieldMapper.Resolution.NANOSECONDS) {
                    throw new IllegalArgumentException(
                        "DateNanosQueryList only supports date_nanos fields, but got: " + field.typeName() + " for field: " + field.name()
                    );
                }
                this.dateFieldType = dateFieldType;
            } else {
                throw new IllegalArgumentException(
                    "DateNanosQueryList only supports date_nanos fields, but got: " + field.typeName() + " for field: " + field.name()
                );
            }
        }

        @Override
        public DateNanosQueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
            return new DateNanosQueryList(
                field,
                searchExecutionContext,
                (LongBlock) block,
                new OnlySingleValueParams(warnings, multiValueWarningMessage)
            );
        }

        @Override
        Query doGetQuery(int position, int firstValueIndex, int valueCount) {
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> dateFieldType.equalityQuery(blockValueReader.apply(firstValueIndex), searchExecutionContext);
                default -> {
                    // The following code is a slight simplification of the DateFieldMapper.termsQuery method
                    final Set<Long> values = new HashSet<>(valueCount);
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (int i = 0; i < valueCount; i++) {
                        final Long value = blockValueReader.apply(firstValueIndex + i);
                        if (values.contains(value)) {
                            continue; // Skip duplicates
                        }
                        values.add(value);
                        builder.add(dateFieldType.equalityQuery(value, searchExecutionContext), BooleanClause.Occur.SHOULD);
                    }
                    yield new ConstantScoreQuery(builder.build());
                }
            };
        }
    }

    private static class GeoShapeQueryList extends QueryList {
        private final BytesRef scratch = new BytesRef();
        private final IntFunction<Geometry> blockValueReader;
        private final IntFunction<Query> shapeQuery;

        private GeoShapeQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            AliasFilter aliasFilter,
            Block block,
            OnlySingleValueParams onlySingleValueParams
        ) {
            super(field, searchExecutionContext, aliasFilter, block, onlySingleValueParams);

            this.blockValueReader = blockToGeometry(block);
            this.shapeQuery = shapeQuery();
        }

        @Override
        public GeoShapeQueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
            return new GeoShapeQueryList(
                field,
                searchExecutionContext,
                aliasFilter,
                block,
                new OnlySingleValueParams(warnings, multiValueWarningMessage)
            );
        }

        @Override
        Query doGetQuery(int position, int firstValueIndex, int valueCount) {
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> shapeQuery.apply(firstValueIndex);
                // TODO: support multiple values
                default -> throw new IllegalArgumentException("can't read multiple Geometry values from a single position");
            };
        }

        private IntFunction<Geometry> blockToGeometry(Block block) {
            return switch (block.elementType()) {
                case LONG -> offset -> {
                    var encoded = ((LongBlock) block).getLong(offset);
                    return new Point(
                        GeoEncodingUtils.decodeLongitude((int) encoded),
                        GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32))
                    );
                };
                case BYTES_REF -> offset -> {
                    var wkb = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                    return WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                };
                case NULL -> offset -> null;
                default -> throw new IllegalArgumentException("can't read Geometry values from [" + block.elementType() + "] block");
            };
        }

        private IntFunction<Query> shapeQuery() {
            if (field instanceof GeoShapeQueryable geoShapeQueryable) {
                return offset -> geoShapeQueryable.geoShapeQuery(
                    searchExecutionContext,
                    field.name(),
                    ShapeRelation.INTERSECTS,
                    blockValueReader.apply(offset)
                );
            }
            // TODO: Support cartesian ShapeQueryable
            throw new IllegalArgumentException("Unsupported field type for geo_match ENRICH: " + field.typeName());
        }
    }

    protected record OnlySingleValueParams(Warnings warnings, String multiValueWarningMessage) {}
}
