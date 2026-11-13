/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_VARIABLES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_VERSION_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A reference to a server-side prompt template plus the variables to render it with. It is an alternative to
 * {@code messages} in a chat completion request. Any chat completion endpoint of a service that hosts templates accepts
 * either shape; endpoints that must not receive caller-authored messages advertise a template policy of {@code required}
 * and accept only this one. Elasticsearch only checks the shape of this object and forwards it unchanged; the owning service
 * validates {@code variables} against the template's schema and renders the prompt.
 *
 * @param id        the template identifier, which the caller pins together with {@code version}
 * @param version   the template version, a positive integer
 * @param variables caller supplied data for the template, possibly empty, never {@code null}
 */
public record Template(String id, int version, Map<String, Object> variables) implements Accountable, ToXContentObject, Writeable {

    public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
        Template.class.getSimpleName(),
        args -> new Template((String) args[0], (Integer) args[1], castVariables(args[2]))
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(TEMPLATE_ID_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(TEMPLATE_VERSION_FIELD));
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField(TEMPLATE_VARIABLES_FIELD));
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Template.class);

    public Template {
        Objects.requireNonNull(id);
        Objects.requireNonNull(variables);
    }

    public Template(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readGenericMap());
    }

    /**
     * The parser hands back the map produced by {@code XContentParser#map()}, which is always {@code Map<String, Object>};
     * the cast only removes the raw type of the parser's argument array.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> castVariables(Object variables) {
        return (Map<String, Object>) variables;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVInt(version);
        out.writeGenericMap(variables);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEMPLATE_ID_FIELD, id);
        builder.field(TEMPLATE_VERSION_FIELD, version);
        builder.field(TEMPLATE_VARIABLES_FIELD, variables);
        builder.endObject();
        return builder;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(id) + sizeOfVariables(variables);
    }

    /**
     * Walks the whole value tree. {@link RamUsageEstimator#sizeOfMap(Map)} stops at depth one, which would leave the strings inside a
     * list of objects, the usual shape of template variables, uncounted. The estimate errs on the high side so the circuit
     * breaker contract (estimate at least the real footprint) holds.
     */
    private static long sizeOfVariables(@Nullable Object value) {
        return switch (value) {
            case null -> 0L;
            case String string -> RamUsageEstimator.sizeOf(string);
            case Map<?, ?> map -> {
                long size = RamUsageEstimator.alignObjectSize(
                    RamUsageEstimator.shallowSizeOf(map) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) map.size() * MAP_ENTRY_SIZE
                );
                for (var entry : map.entrySet()) {
                    size += sizeOfVariables(entry.getKey()) + sizeOfVariables(entry.getValue());
                }
                yield size;
            }
            case Collection<?> collection -> {
                long size = RamUsageEstimator.alignObjectSize(
                    RamUsageEstimator.shallowSizeOf(collection) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) collection.size()
                        * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                );
                for (var element : collection) {
                    size += sizeOfVariables(element);
                }
                yield size;
            }
            // Scalars: the boxed numbers and booleans the parser produces, and anything exotic such as binary from a non-JSON body.
            // All are leaves with no children, and an estimate is always preferable to rejecting a request here.
            default -> RamUsageEstimator.shallowSizeOf(value);
        };
    }

    /**
     * A hash map entry holds the key and value references, the cached hash and the next pointer.
     */
    private static final long MAP_ENTRY_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES
    );
}
