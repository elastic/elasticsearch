/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorFormat;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.lookup.Source;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;

/**
 * A {@link SourceValueFetcher} for {@code dense_vector} fields.
 */
class DenseVectorSourceValueFetcher extends SourceValueFetcher {
    private static final Logger logger = LogManager.getLogger(DenseVectorSourceValueFetcher.class);

    private final String fieldName;
    private final Set<String> sourcePaths;
    private final ElementType elementType;
    private final int dims;
    private final VectorFormat format;

    DenseVectorSourceValueFetcher(
        String fieldName,
        SearchExecutionContext context,
        ElementType elementType,
        int dims,
        VectorFormat format
    ) {
        this(
            fieldName,
            context.isSourceEnabled() ? context.sourcePath(fieldName) : Set.of(),
            context.getIndexSettings().getIgnoredSourceFormat(),
            elementType,
            dims,
            format
        );
    }

    DenseVectorSourceValueFetcher(
        String fieldName,
        Set<String> sourcePaths,
        IgnoredSourceFormat ignoredSourceFormat,
        ElementType elementType,
        int dims,
        VectorFormat format
    ) {
        super(sourcePaths, null, ignoredSourceFormat);
        this.fieldName = fieldName;
        this.sourcePaths = sourcePaths;
        this.elementType = elementType;
        this.dims = dims;
        this.format = format;
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
        // A dense_vector holds exactly one vector, so only one value is returned. Additional values are
        // only reachable when this field is the target of a copy_to; the value assigned directly to the
        // field wins over a copied one, and the rest are reported as ignored.
        List<Object> values = null;
        String winningPath = null;
        Object winningValue = null;
        for (var path : sourcePaths) {
            Object sourceValue = source.extractValue(path, null);
            if (sourceValue == null) {
                continue;
            }

            if (values != null && path.equals(fieldName) == false) {
                ignore(
                    ignoredValues,
                    sourceValue,
                    path,
                    new IllegalStateException("a dense_vector holds a single value and one has already been found"),
                    true
                );
                continue;
            }

            try {
                List<Object> parsed = switch (format) {
                    case ARRAY -> arrayValues(sourceValue);
                    case BINARY -> binaryValues(sourceValue);
                };
                if (values != null) {
                    ignore(
                        ignoredValues,
                        winningValue,
                        winningPath,
                        new IllegalStateException("a dense_vector holds a single value and one has already been found"),
                        true
                    );
                }
                values = parsed;
                winningPath = path;
                winningValue = sourceValue;
            } catch (Exception e) {
                // if parsing fails here then it would have failed at index time
                // as well, meaning that we must be ignoring malformed values.
                ignore(ignoredValues, sourceValue, path, e, false);
            }
        }
        return values == null ? List.of() : values;
    }

    private static void ignore(List<Object> ignoredValues, Object value, String path, Exception reason, boolean logAsWarning) {
        final Supplier<String> messageSupplier = () -> format("ignoring dense vector value from source path [%s]", path);

        ignoredValues.add(value);
        if (logAsWarning) {
            logger.warn(messageSupplier, reason);
        } else {
            logger.debug(messageSupplier, reason);
        }
    }

    /**
     * Decodes source values to a list of numbers. Used for {@code format: "array"}.
     * Returns {@code Integer} components for byte and bit fields, {@code Float} otherwise.
     */
    private List<Object> arrayValues(Object sourceValue) {
        switch (sourceValue) {
            case List<?> v -> {
                List<Object> values = new ArrayList<>(v.size());
                for (Object o : v) {
                    values.add(switch (elementType) {
                        case BYTE, BIT -> NumberFieldMapper.NumberType.BYTE.parse(o, false).intValue();
                        case FLOAT, BFLOAT16 -> NumberFieldMapper.NumberType.FLOAT.parse(o, false);
                    });
                }
                return values;
            }
            case String s -> {
                return DecodedVector.decode(s, elementType, dims, parseHexStrings()).toValueList();
            }
            default -> throw unsupportedSourceValue(sourceValue);
        }
    }

    /**
     * Encodes source values as a single-element list containing a base64 string. Used for {@code format: "binary"}.
     * For bfloat16-encoded input, each component is widened to 4 bytes (float32) before encoding.
     */
    private List<Object> binaryValues(Object sourceValue) {
        switch (sourceValue) {
            case List<?> v -> {
                return List.of(encodeBase64(v, elementType));
            }
            case String s -> {
                return List.of(DecodedVector.decode(s, elementType, dims, parseHexStrings()).toBase64());
            }
            default -> throw unsupportedSourceValue(sourceValue);
        }
    }

    /**
     * Encodes source values as base64 using the canonical binary form for {@code elementType}: one byte per
     * component for byte and bit vectors, four big-endian bytes otherwise.
     */
    private static String encodeBase64(List<?> values, ElementType elementType) {
        return switch (elementType) {
            case BYTE, BIT -> {
                byte[] encoded = new byte[values.size()];
                int i = 0;
                for (Object value : values) {
                    encoded[i++] = NumberFieldMapper.NumberType.BYTE.parse(value, false).byteValue();
                }
                yield Base64.getEncoder().encodeToString(encoded);
            }
            case FLOAT, BFLOAT16 -> {
                ByteBuffer buffer = ByteBuffer.allocate(values.size() * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
                for (Object value : values) {
                    buffer.putFloat(NumberFieldMapper.NumberType.FLOAT.parse(value, false).floatValue());
                }
                yield Base64.getEncoder().encodeToString(buffer.array());
            }
        };
    }

    private boolean parseHexStrings() {
        return elementType == ElementType.BYTE || elementType == ElementType.BIT;
    }

    private static IllegalArgumentException unsupportedSourceValue(Object sourceValue) {
        return new IllegalArgumentException("unsupported source value type [" + sourceValue.getClass().getSimpleName() + "]");
    }

    @Override
    protected Object parseSourceValue(Object value) {
        throw new IllegalStateException("parsing dense vector from source is not supported here");
    }
}
