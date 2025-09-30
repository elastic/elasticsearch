/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Simplified XContentParser for flattened ESON structures.
 *
 * This parser assumes the ESON has been flattened using ESONSource.flatten(),
 * which means all nested structures are expanded into a single linear key array.
 *
 * The parser performs a single iteration through the key array, using a stack
 * to track container boundaries and maintain proper state.
 */
public class ESONXContentSerializer {

    // Stack to track which containers we're currently inside
    private static class ContainerState {
        final boolean isArray;
        int remainingFields;

        ContainerState(boolean isArray, int fieldCount) {
            this.isArray = isArray;
            this.remainingFields = fieldCount;
        }
    }

    /**
     * Efficient toXContent for flattened ESON structures.
     * Performs a single pass through the key array with a stack to track nesting.
     */
    public static XContentBuilder flattenToXContent(ESONFlat esonFlat, XContentBuilder builder, ToXContent.Params params)
        throws IOException {
        List<ESONEntry> keyArray = esonFlat.getKeys();
        ESONSource.Values values = esonFlat.values();
        ESONEntry.ObjectEntry rootObjEntry = (ESONEntry.ObjectEntry) keyArray.get(0);

        Deque<ContainerState> containerStack = new ArrayDeque<>();

        // Start with root object
        builder.startObject();
        containerStack.push(new ContainerState(false, rootObjEntry.offsetOrCount()));

        int index = 1; // Skip root ObjectEntry at index 0

        while (index < keyArray.size() && containerStack.isEmpty() == false) {
            ESONEntry entry = keyArray.get(index);
            ContainerState currentContainer = containerStack.peek();

            // Check if we need to close any containers
            while (currentContainer != null && currentContainer.remainingFields == 0) {
                containerStack.pop();
                if (currentContainer.isArray) {
                    builder.endArray();
                } else {
                    builder.endObject();
                }

                // Update parent container's remaining count
                if (containerStack.isEmpty() == false) {
                    containerStack.peek().remainingFields--;
                }

                currentContainer = containerStack.peek();
            }

            if (containerStack.isEmpty()) {
                break;
            }

            // Process the current entry
            if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                // Simple field with a value
                if (currentContainer.isArray == false && fieldEntry.key() != null) {
                    builder.field(fieldEntry.key());
                }

                writeValue(values, builder, fieldEntry.value(), params);
                currentContainer.remainingFields--;
                index++;

            } else if (entry instanceof ESONEntry.ObjectEntry objEntry) {
                // Nested object
                if (currentContainer.isArray == false && objEntry.key() != null) {
                    builder.field(objEntry.key());
                }

                builder.startObject();
                containerStack.push(new ContainerState(false, objEntry.offsetOrCount()));
                index++;

            } else if (entry instanceof ESONEntry.ArrayEntry arrEntry) {
                // Nested array
                if (currentContainer.isArray == false && arrEntry.key() != null) {
                    builder.field(arrEntry.key());
                }

                builder.startArray();
                containerStack.push(new ContainerState(true, arrEntry.offsetOrCount()));
                index++;
            }
        }

        // Close any remaining containers
        while (containerStack.isEmpty() == false) {
            ContainerState container = containerStack.pop();
            if (container.isArray) {
                builder.endArray();
            } else {
                builder.endObject();
            }
        }

        return builder;
    }

    /**
     * Helper method to write a value to the XContentBuilder
     */
    private static void writeValue(ESONSource.Values values, XContentBuilder builder, ESONSource.Value type, ToXContent.Params params)
        throws IOException {
        if (type == null || type == ESONSource.ConstantValue.NULL) {
            builder.nullValue();
        } else if (type == ESONSource.ConstantValue.TRUE || type == ESONSource.ConstantValue.FALSE) {
            builder.value(type == ESONSource.ConstantValue.TRUE);
        } else if (type instanceof ESONSource.Mutation mutation) {
            throw new IllegalStateException("Should not have mutation in flattened ESON " + mutation);
        } else if (type instanceof ESONSource.FixedValue fixed) {
            fixed.writeToXContent(builder, values);
        } else if (type instanceof ESONSource.VariableValue var) {
            var.writeToXContent(builder, values);
        } else {
            throw new IllegalStateException("Unknown type: " + type.getClass());
        }
    }

    /**
     * Helper method to write a mutated object value
     */
    private static void writeObject(XContentBuilder builder, Object obj, ToXContent.Params params) throws IOException {
        if (obj == null) {
            builder.nullValue();
        } else if (obj instanceof String str) {
            builder.value(str);
        } else if (obj instanceof Number num) {
            builder.value(num);
        } else if (obj instanceof Boolean bool) {
            builder.value(bool);
        } else if (obj instanceof byte[] bytes) {
            builder.value(bytes);
        } else if (obj instanceof Map<?, ?> map) {
            builder.startObject();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                builder.field(entry.getKey().toString());
                writeObject(builder, entry.getValue(), params);
            }
            builder.endObject();
        } else if (obj instanceof List<?> list) {
            builder.startArray();
            for (Object item : list) {
                writeObject(builder, item, params);
            }
            builder.endArray();
        } else if (obj instanceof ToXContent toXContent) {
            toXContent.toXContent(builder, params);
        } else {
            builder.value(obj.toString());
        }
    }
}
