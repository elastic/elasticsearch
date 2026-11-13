/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.support.AbstractXContentParser;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Reads mapped nested objects from a parent's stored source. Shared by NESTED reductions and NESTED_EXPAND. */
final class NestedSource {
    private NestedSource() {}

    static BytesRef singleMetadata(Block block, int row, String name) {
        if (block instanceof BytesRefBlock == false || block.getValueCount(row) != 1) {
            throw new IllegalArgumentException("NESTED requires exactly one " + name + " metadata value per parent row");
        }
        return ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(row), new BytesRef());
    }

    /** Every object of the mapped collection at {@code path}, in source order. Null slots are skipped. */
    static List<Map<?, ?>> objects(String path, BytesRef source, ParsedContentReservation parsedSource, DriverContext context) {
        Object value = XContentMapValues.extractValue(path, decode(source, parsedSource, context));
        if (value == null) {
            return List.of();
        }
        if (value instanceof Map<?, ?> map) {
            context.checkForEarlyTermination();
            return List.of(map);
        }
        if (value instanceof List<?> list) {
            List<Map<?, ?>> objects = new ArrayList<>();
            // Indexing flattens arrays of arrays too. Walk iteratively to retain every object without recursive stack growth.
            ArrayDeque<Iterator<?>> pending = new ArrayDeque<>();
            pending.push(list.iterator());
            int visited = 0;
            while (pending.isEmpty() == false) {
                Iterator<?> iterator = pending.peek();
                if (iterator.hasNext() == false) {
                    pending.pop();
                    continue;
                }
                if ((visited++ & 1023) == 0) {
                    context.checkForEarlyTermination();
                }
                Object item = iterator.next();
                if (item instanceof Map<?, ?> object) {
                    objects.add(object);
                } else if (item instanceof List<?> nested) {
                    pending.push(nested.iterator());
                } else if (item != null) {
                    throw new IllegalArgumentException("mapped nested path [" + path + "] contains a non-object value");
                }
            }
            return objects;
        }
        throw new IllegalArgumentException("mapped nested path [" + path + "] is not an object collection");
    }

    private static Map<String, Object> decode(BytesRef source, ParsedContentReservation parsedSource, DriverContext context) {
        try (
            var parser = new FilterXContentParserWrapper(
                parsedSource.track(XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(source)))
            ) {
                private int tokens;

                @Override
                public Token nextToken() throws IOException {
                    if ((++tokens & 1023) == 0) {
                        context.checkForEarlyTermination();
                    }
                    return super.nextToken();
                }
            }
        ) {
            // Read through the wrapper: delegating map() would bypass token-level cancellation.
            var decoded = AbstractXContentParser.readMap(parser, HashMap::new);
            // Small parses flush without a limit check. Admit their retained charge before constructing child channels.
            context.breaker().addEstimateBytesAndMaybeBreak(0, ParsedContentReservation.LABEL);
            return decoded;
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse NESTED source", e);
        }
    }

    /** The value of one child field in one object, converted to the mapped ES|QL type. */
    static Object childValue(Map<?, ?> object, String path, DataType type, DriverContext context) {
        return coerce(XContentMapValues.extractValue(path, object), type, context);
    }

    private static Object coerce(Object value, DataType type, DriverContext context) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            // Flatten this field within one child object, matching indexing's array semantics without mixing sibling objects.
            List<Object> values = new ArrayList<>();
            ArrayDeque<Iterator<?>> pending = new ArrayDeque<>();
            pending.push(list.iterator());
            int visited = 0;
            while (pending.isEmpty() == false) {
                Iterator<?> iterator = pending.peek();
                if (iterator.hasNext() == false) {
                    pending.pop();
                    continue;
                }
                if ((visited++ & 1023) == 0) {
                    context.checkForEarlyTermination();
                }
                Object item = iterator.next();
                if (item instanceof List<?> nested) {
                    pending.push(nested.iterator());
                } else if (item != null) {
                    Object converted = coerceScalar(item, type);
                    if (converted != null) {
                        values.add(converted);
                    }
                }
            }
            return values.isEmpty() ? null : values;
        }
        return coerceScalar(value, type);
    }

    private static Object coerceScalar(Object value, DataType type) {
        return switch (type) {
            case LONG -> "".equals(value) ? null : NumberFieldMapper.NumberType.LONG.parse(value, true);
            case INTEGER -> "".equals(value) ? null : NumberFieldMapper.NumberType.INTEGER.parse(value, true);
            case DOUBLE -> "".equals(value) ? null : NumberFieldMapper.NumberType.DOUBLE.parse(value, true);
            case BOOLEAN -> value instanceof Boolean b ? b : Booleans.parseBoolean(value.toString(), false);
            case KEYWORD, TEXT -> new BytesRef(value.toString());
            default -> throw new IllegalArgumentException("unsupported NESTED child type [" + type + "]");
        };
    }
}
