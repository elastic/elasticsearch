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
import org.elasticsearch.compute.data.Page;
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
import java.util.function.BiFunction;

/**
 * Generates a list of Lucene queries based on the input block.
 * Each QueryList stores a channel offset to extract the correct Block from the Page.
 */
public abstract class QueryList implements LookupEnrichQueryGenerator {
    protected final SearchExecutionContext searchExecutionContext;
    protected final AliasFilter aliasFilter;
    protected final MappedFieldType field;
    protected final int channelOffset; // Channel index in the input Page
    @Nullable
    protected final OnlySingleValueParams onlySingleValueParams;

    protected QueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset,
        OnlySingleValueParams onlySingleValueParams
    ) {
        this.searchExecutionContext = searchExecutionContext;
        this.aliasFilter = aliasFilter;
        this.field = field;
        this.channelOffset = channelOffset;
        this.onlySingleValueParams = onlySingleValueParams;
    }

    /**
     * Returns the number of positions in this query list (same as input block position count).
     */
    @Override
    public int getPositionCount(Page inputPage) {
        return inputPage.getBlock(channelOffset).getPositionCount();
    }

    /**
     * Returns a copy of this query list that only returns queries for single-valued positions.
     * That is, it returns `null` queries for either multivalued or null positions.
     * <p>
     * Whenever a multi-value position is encountered, whether in the input block or in the queried index, a warning is emitted.
     * </p>
     */
    public abstract QueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage);

    @Override
    public final Query getQuery(int position, Page inputPage) {
        Block inputBlock = inputPage.getBlock(channelOffset);
        final int valueCount = inputBlock.getValueCount(position);
        if (onlySingleValueParams != null && valueCount != 1) {
            if (valueCount > 1) {
                onlySingleValueParams.warnings.registerException(
                    new IllegalArgumentException(onlySingleValueParams.multiValueWarningMessage)
                );
            }
            return null;
        }
        final int firstValueIndex = inputBlock.getFirstValueIndex(position);

        Query query = doGetQuery(position, firstValueIndex, valueCount, inputBlock);

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
    public abstract Query doGetQuery(int position, int firstValueIndex, int valueCount, Block inputBlock);

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
     * Returns a function that reads values from a block given its element type.
     * The function takes the block and offset, and returns the value as an {@link Object}.
     */
    public static BiFunction<Block, Integer, Object> createBlockValueReaderForType(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN -> (block, offset) -> ((BooleanBlock) block).getBoolean(offset);
            case BYTES_REF -> (block, offset) -> ((BytesRefBlock) block).getBytesRef(offset, new BytesRef());
            case DOUBLE -> (block, offset) -> ((DoubleBlock) block).getDouble(offset);
            case FLOAT -> (block, offset) -> ((FloatBlock) block).getFloat(offset);
            case LONG -> (block, offset) -> ((LongBlock) block).getLong(offset);
            case INT -> (block, offset) -> ((IntBlock) block).getInt(offset);
            case NULL -> (block, offset) -> null;
            case DOC -> throw new IllegalArgumentException("can't read values from [doc] block");
            case COMPOSITE -> throw new IllegalArgumentException("can't read values from [composite] block");
            case AGGREGATE_METRIC_DOUBLE -> throw new IllegalArgumentException("can't read values from [aggregate metric double] block");
            case EXPONENTIAL_HISTOGRAM -> throw new IllegalArgumentException("can't read values from [exponential histogram] block");
            case TDIGEST -> throw new IllegalArgumentException("can't read values from [tdigest] block");
            case UNKNOWN -> (block, offset) -> { throw new IllegalArgumentException("can't read values from [" + block + "]"); };
        };
    }

    /**
     * Returns a list of term queries for the given field and element type.
     */
    public static QueryList rawTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset,
        ElementType elementType
    ) {
        return new TermQueryList(
            field,
            searchExecutionContext,
            aliasFilter,
            channelOffset,
            null,
            createBlockValueReaderForType(elementType)
        );
    }

    /**
     * Returns a list of term queries for the given field for IP values.
     */
    public static QueryList ipTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset
    ) {
        BytesRef scratch = new BytesRef();
        byte[] ipBytes = new byte[InetAddressPoint.BYTES];
        return new TermQueryList(field, searchExecutionContext, aliasFilter, channelOffset, null, (block, offset) -> {
            BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
            final var bytes = bytesRefBlock.getBytesRef(offset, scratch);
            if (ipBytes.length != bytes.length) {
                // Lucene only support 16-byte IP addresses, even IPv4 is encoded in 16 bytes
                throw new IllegalStateException("Cannot decode IP field from bytes of length " + bytes.length);
            }
            System.arraycopy(bytes.bytes, bytes.offset, ipBytes, 0, bytes.length);
            return InetAddressPoint.decode(ipBytes);
        });
    }

    /**
     * Returns a list of term queries for the given field for date values.
     */
    public static QueryList dateTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset
    ) {
        return new TermQueryList(
            field,
            searchExecutionContext,
            aliasFilter,
            channelOffset,
            null,
            field instanceof RangeFieldMapper.RangeFieldType rangeFieldType
                ? (block, offset) -> rangeFieldType.dateTimeFormatter().formatMillis(((LongBlock) block).getLong(offset))
                : (block, offset) -> ((LongBlock) block).getLong(offset)
        );
    }

    /**
     * Returns a list of term queries for the given field for date_nanos values.
     */
    public static QueryList dateNanosTermQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset
    ) {
        return new DateNanosQueryList(field, searchExecutionContext, aliasFilter, channelOffset, null);
    }

    /**
     * Returns a list of geo_shape queries for the given field.
     */
    public static QueryList geoShapeQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset
    ) {
        return new GeoShapeQueryList(field, searchExecutionContext, aliasFilter, channelOffset, null);
    }

    private static class TermQueryList extends QueryList {
        private final BiFunction<Block, Integer, Object> blockValueReader;

        private TermQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            AliasFilter aliasFilter,
            int channelOffset,
            OnlySingleValueParams onlySingleValueParams,
            BiFunction<Block, Integer, Object> blockValueReader
        ) {
            super(field, searchExecutionContext, aliasFilter, channelOffset, onlySingleValueParams);
            this.blockValueReader = blockValueReader;
        }

        @Override
        public TermQueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
            return new TermQueryList(
                field,
                searchExecutionContext,
                aliasFilter,
                channelOffset,
                new OnlySingleValueParams(warnings, multiValueWarningMessage),
                blockValueReader
            );
        }

        @Override
        public Query doGetQuery(int position, int firstValueIndex, int valueCount, Block inputBlock) {
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> field.termQuery(blockValueReader.apply(inputBlock, firstValueIndex), searchExecutionContext);
                default -> {
                    final List<Object> terms = new ArrayList<>(valueCount);
                    for (int i = 0; i < valueCount; i++) {
                        final Object value = blockValueReader.apply(inputBlock, firstValueIndex + i);
                        terms.add(value);
                    }
                    yield field.termsQuery(terms, searchExecutionContext);
                }
            };
        }
    }

    private static class DateNanosQueryList extends QueryList {
        private final DateFieldMapper.DateFieldType dateFieldType;

        private DateNanosQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            AliasFilter aliasFilter,
            int channelOffset,
            OnlySingleValueParams onlySingleValueParams
        ) {
            super(field, searchExecutionContext, aliasFilter, channelOffset, onlySingleValueParams);
            if (field instanceof RangeFieldMapper.RangeFieldType rangeFieldType) {
                // TODO: do this validation earlier
                throw new IllegalArgumentException(
                    "DateNanosQueryList does not support range fields [" + rangeFieldType + "]: " + field.name()
                );
            }
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
                aliasFilter,
                channelOffset,
                new OnlySingleValueParams(warnings, multiValueWarningMessage)
            );
        }

        @Override
        public Query doGetQuery(int position, int firstValueIndex, int valueCount, Block inputBlock) {
            LongBlock longBlock = (LongBlock) inputBlock;
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> dateFieldType.equalityQuery(longBlock.getLong(firstValueIndex), searchExecutionContext);
                default -> {
                    // The following code is a slight simplification of the DateFieldMapper.termsQuery method
                    final Set<Long> values = new HashSet<>(valueCount);
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (int i = 0; i < valueCount; i++) {
                        final Long value = longBlock.getLong(firstValueIndex + i);
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

        private GeoShapeQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            AliasFilter aliasFilter,
            int channelOffset,
            OnlySingleValueParams onlySingleValueParams
        ) {
            super(field, searchExecutionContext, aliasFilter, channelOffset, onlySingleValueParams);
        }

        @Override
        public GeoShapeQueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
            return new GeoShapeQueryList(
                field,
                searchExecutionContext,
                aliasFilter,
                channelOffset,
                new OnlySingleValueParams(warnings, multiValueWarningMessage)
            );
        }

        @Override
        public Query doGetQuery(int position, int firstValueIndex, int valueCount, Block inputBlock) {
            return switch (valueCount) {
                case 0 -> null;
                case 1 -> {
                    Geometry geometry = blockToGeometry(inputBlock, firstValueIndex);
                    yield shapeQuery(geometry);
                }
                // TODO: support multiple values
                default -> throw new IllegalArgumentException("can't read multiple Geometry values from a single position");
            };
        }

        private Geometry blockToGeometry(Block block, int offset) {
            return switch (block.elementType()) {
                case LONG -> {
                    var encoded = ((LongBlock) block).getLong(offset);
                    yield new Point(
                        GeoEncodingUtils.decodeLongitude((int) encoded),
                        GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32))
                    );
                }
                case BYTES_REF -> {
                    var wkb = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                    yield WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                }
                case NULL -> null;
                default -> throw new IllegalArgumentException("can't read Geometry values from [" + block.elementType() + "] block");
            };
        }

        private Query shapeQuery(Geometry geometry) {
            if (field instanceof GeoShapeQueryable geoShapeQueryable) {
                return geoShapeQueryable.geoShapeQuery(searchExecutionContext, field.name(), ShapeRelation.INTERSECTS, geometry);
            }
            // TODO: Support cartesian ShapeQueryable
            throw new IllegalArgumentException("Unsupported field type for geo_match ENRICH: " + field.typeName());
        }
    }

    public record OnlySingleValueParams(Warnings warnings, String multiValueWarningMessage) {}
}
