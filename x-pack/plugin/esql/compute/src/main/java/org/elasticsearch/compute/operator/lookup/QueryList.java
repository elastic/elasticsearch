/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Generates a list of Lucene queries based on the input block.
 */
public abstract class QueryList {
    protected final Block block;

    protected QueryList(Block block) {
        this.block = block;
    }

    /**
     * Returns the number of positions in this query list
     */
    int getPositionCount() {
        return block.getPositionCount();
    }

    /**
     * Returns the query at the given position.
     */
    @Nullable
    abstract Query getQuery(int position);

    /**
     * Returns a list of term queries for the given field and the input block
     * using only the {@link ElementType} of the {@link Block} to determine the
     * query.
     */
    public static QueryList rawTermQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, Block block) {
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
            case UNKNOWN -> throw new IllegalArgumentException("can't read values from [" + block + "]");
        };
        return new TermQueryList(field, searchExecutionContext, block, blockToJavaObject);
    }

    /**
     * Returns a list of term queries for the given field and the input block of
     * {@code ip} field values.
     */
    public static QueryList ipTermQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, BytesRefBlock block) {
        BytesRef scratch = new BytesRef();
        byte[] ipBytes = new byte[InetAddressPoint.BYTES];
        return new TermQueryList(field, searchExecutionContext, block, offset -> {
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
    public static QueryList dateTermQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, LongBlock block) {
        return new TermQueryList(
            field,
            searchExecutionContext,
            block,
            field instanceof RangeFieldMapper.RangeFieldType rangeFieldType
                ? offset -> rangeFieldType.dateTimeFormatter().formatMillis(block.getLong(offset))
                : block::getLong
        );
    }

    /**
     * Returns a list of geo_shape queries for the given field and the input block.
     */
    public static QueryList geoShapeQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, Block block) {
        return new GeoShapeQueryList(field, searchExecutionContext, block);
    }

    private static class TermQueryList extends QueryList {
        private final MappedFieldType field;
        private final SearchExecutionContext searchExecutionContext;
        private final IntFunction<Object> blockValueReader;

        private TermQueryList(
            MappedFieldType field,
            SearchExecutionContext searchExecutionContext,
            Block block,
            IntFunction<Object> blockValueReader
        ) {
            super(block);
            this.field = field;
            this.searchExecutionContext = searchExecutionContext;
            this.blockValueReader = blockValueReader;
        }

        @Override
        Query getQuery(int position) {
            final int first = block.getFirstValueIndex(position);
            final int count = block.getValueCount(position);
            return switch (count) {
                case 0 -> null;
                case 1 -> field.termQuery(blockValueReader.apply(first), searchExecutionContext);
                default -> {
                    final List<Object> terms = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        final Object value = blockValueReader.apply(first + i);
                        terms.add(value);
                    }
                    yield field.termsQuery(terms, searchExecutionContext);
                }
            };
        }
    }

    private static class GeoShapeQueryList extends QueryList {
        private final BytesRef scratch = new BytesRef();
        private final MappedFieldType field;
        private final SearchExecutionContext searchExecutionContext;
        private final IntFunction<Geometry> blockValueReader;
        private final IntFunction<Query> shapeQuery;

        private GeoShapeQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, Block block) {
            super(block);

            this.field = field;
            this.searchExecutionContext = searchExecutionContext;
            this.blockValueReader = blockToGeometry(block);
            this.shapeQuery = shapeQuery();
        }

        @Override
        Query getQuery(int position) {
            final int first = block.getFirstValueIndex(position);
            final int count = block.getValueCount(position);
            return switch (count) {
                case 0 -> null;
                case 1 -> shapeQuery.apply(first);
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
}
