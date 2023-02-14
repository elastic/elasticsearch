/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class Engine implements Writeable, ToXContentObject {
    private final String name;
    private final String[] indices;

    /**
     * Public constructor.
     *
     * @param name The name of the engine.
     * @param indices The list of indices targeted by this engine.
     */
    public Engine(String name, String[] indices) {
        this.name = name;
        this.indices = indices;
    }

    public Engine(StreamInput in) throws IOException {
        this.name = in.readString();
        this.indices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(indices);
    }

    private static final ConstructingObjectParser<Engine, String> PARSER = new ConstructingObjectParser<>(
        "engine",
        false,
        (params, name) -> {
            @SuppressWarnings("unchecked")
            String[] indices = ((List<String>) params[1]).toArray(String[]::new);
            return new Engine(name, indices);
        }
    );
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField INDICES_FIELD = new ParseField("indices");

    static {
        PARSER.declareString(constructorArg(), NAME_FIELD);
        PARSER.declareStringArray(constructorArg(), INDICES_FIELD);
    }

    /**
     * Parses a {@link Engine} from its {@param xContentType} representation in bytes.
     */
    public static Engine fromXContentBytes(String name, BytesReference source, XContentType xContentType) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                xContentType
            )
        ) {
            return Engine.fromXContent(name, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse the get result", e);
        }
    }

    /**
     * Parses an engine through the provided {@param parser}.
     */
    public static Engine fromXContent(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, name);
    }

    /**
     * Converts the {@link Engine} to XContent.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("indices", indices);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the name of the {@link Engine}.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the list of indices targeted by the {@link Engine}.
     */
    public String[] indices() {
        return indices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Engine engine = (Engine) o;
        return name.equals(engine.name) && Arrays.equals(indices, engine.indices);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Returns the merged {@link Engine} from the current state and the provided {@param update}.
     * This function returns the current instance if the update is a noop.
     */
    Engine merge(BytesReference update, XContentType xContentType, BigArrays bigArrays) {
        final Tuple<XContentType, Map<String, Object>> sourceAndContent;
        try (ReleasableBytesStreamOutput sourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, sourceBuffer)) {
                toXContent(builder, EMPTY_PARAMS);
            }
            sourceAndContent = XContentHelper.convertToMap(sourceBuffer.bytes(), true, XContentType.JSON);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final Tuple<XContentType, Map<String, Object>> updateAndContent = XContentHelper.convertToMap(update, true);
        final Map<String, Object> newSourceAsMap = new HashMap<>(sourceAndContent.v2());
        final boolean noop = XContentHelper.update(newSourceAsMap, updateAndContent.v2(), true) == false;
        if (noop) {
            return this;
        }

        try (ReleasableBytesStreamOutput newSourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), newSourceBuffer)) {
                builder.value(newSourceAsMap);
            }
            return Engine.fromXContentBytes("", newSourceBuffer.bytes(), XContentType.JSON);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
