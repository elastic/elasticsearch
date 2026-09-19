/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Configuration needed to transform loaded values into blocks.
 * {@link MappedFieldType}s will find me in
 * {@link MappedFieldType.BlockLoaderContext#blockLoaderFunctionConfig()} and
 * use this configuration to choose the appropriate implementation for
 * transforming loaded values into blocks.
 */
public interface BlockLoaderFunctionConfig {
    /**
     * Name used in descriptions.
     */
    Function function();

    record JustFunction(Function function) implements BlockLoaderFunctionConfig {}

    /**
     * Configuration for loading time-series metadata fields from {@code _source}.
     * Controls which field types to include (dimensions, metrics, or both) and which dimensions to exclude.
     */
    record TimeSeriesMetadata(boolean loadMetricFields, Set<String> skipFieldNames) implements BlockLoaderFunctionConfig {
        @Override
        public Function function() {
            return Function.TIME_SERIES_METADATA;
        }
    }

    /**
     * Configuration for rounding long values to one of a sorted list of points.
     */
    record RoundToLongs(long[] points) implements BlockLoaderFunctionConfig {
        @Override
        public Function function() {
            return Function.ROUND_TO;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(points);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RoundToLongs other && Arrays.equals(points, other.points);
        }
    }

    /**
     * Encodes a decoded {@code geo_point}, given as {@code (x, y)} i.e. longitude then latitude, into a geo-grid cell id.
     * A negative result means the point has no cell, which for a bounded grid means it lies outside the bounds; the
     * loader then emits {@code null}, or drops the value for a multi-valued point.
     * <p>
     * Implementations are supplied by the caller (ES|QL) because the grid libraries, in particular H3 for
     * {@code geohex}, are not all available to the server module. An encoder may keep per-instance scratch state, so the
     * loader obtains a fresh one from {@link GeoGrid#encoders()} for every reader it creates.
     */
    @FunctionalInterface
    interface GeoGridEncoder {
        long encode(double longitude, double latitude);
    }

    /**
     * Computes the geo-grid cells intersecting a {@code geo_shape}, given the binary doc value of the field, which
     * holds the indexed triangle tree of all shapes in the document. The result may be truncated to a maximum number
     * of cells, in which case the warning given when the tiler was {@link GeoGridShapeTilerFactory created} is used.
     * Like {@link GeoGridEncoder} the implementation is supplied by the caller (ES|QL), which keeps the algorithm and
     * the cell order identical to evaluating the function on a loaded shape.
     */
    @FunctionalInterface
    interface GeoGridShapeTiler {
        List<Long> cells(BytesRef encodedShape) throws IOException;
    }

    /**
     * Creates a {@link GeoGridShapeTiler} for one reader. The warnings, when not null, receive a warning if the cells
     * of a shape are truncated. Tilers may keep scratch state, so one is created per reader.
     */
    @FunctionalInterface
    interface GeoGridShapeTilerFactory {
        GeoGridShapeTiler create(@Nullable Warnings warnings);
    }

    /**
     * Configuration for loading {@code geo_point} or {@code geo_shape} doc values directly as geo-grid cell ids
     * ({@code ST_GEOHASH}, {@code ST_GEOTILE} or {@code ST_GEOHEX}), optionally restricted to the cells intersecting
     * {@code bounds}. Points use {@link #encoders()} and shapes {@link #shapeTilers()}. Equality deliberately ignores
     * both: they are fully determined by the function, precision and bounds.
     */
    record GeoGrid(
        Function function,
        int precision,
        @Nullable GeoBoundingBox bounds,
        Supplier<GeoGridEncoder> encoders,
        GeoGridShapeTilerFactory shapeTilers
    ) implements BlockLoaderFunctionConfig {
        public GeoGrid {
            if (function != Function.ST_GEOHASH && function != Function.ST_GEOTILE && function != Function.ST_GEOHEX) {
                throw new IllegalArgumentException("not a geo-grid function [" + function + "]");
            }
        }

        @Override
        public int hashCode() {
            // Enum hashCode is identity based; use the name so the hash is stable across JVMs (it ends up in attribute names).
            return Objects.hash(function.name(), precision, bounds);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof GeoGrid other
                && function == other.function
                && precision == other.precision
                && Objects.equals(bounds, other.bounds);
        }
    }

    enum Function {
        AMD_COUNT,
        AMD_DEFAULT,
        AMD_MAX,
        AMD_MIN,
        AMD_SUM,
        BYTE_LENGTH,
        MV_MAX,
        MV_MIN,
        LENGTH,
        ROUND_TO,
        ST_GEOHASH,
        ST_GEOTILE,
        ST_GEOHEX,
        V_COSINE,
        V_DOT_PRODUCT,
        V_HAMMING,
        V_L1NORM,
        V_L2NORM,
        TIME_SERIES_METADATA,
        EXTRACT_FLATTENED_SUBFIELD
    }
}
