/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates a pipeline's id and configuration as a loosely typed map -- see {@link Pipeline} for the
 * parsed and processed object(s) that a pipeline configuration will become. This class is used for things
 * like keeping track of pipelines in the cluster state (where a pipeline is 'just some json') whereas the
 * {@link Pipeline} class is used in the actual processing of ingest documents through pipelines in the
 * {@link IngestService}.
 */
public final class PipelineConfiguration implements SimpleDiffable<PipelineConfiguration>, ToXContentObject {

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("pipeline_config", true, Builder::new);
    static {
        PARSER.declareString(Builder::setId, new ParseField("id"));
        PARSER.declareField(
            (parser, builder, aVoid) -> builder.setConfig(parser.mapOrdered()),
            new ParseField("config"),
            ObjectParser.ValueType.OBJECT
        );
    }

    public static ContextParser<Void, PipelineConfiguration> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }

    private static class Builder {

        private String id;
        private Map<String, Object> config;

        void setId(String id) {
            this.id = id;
        }

        void setConfig(Map<String, Object> config) {
            this.config = config;
        }

        PipelineConfiguration build() {
            return new PipelineConfiguration(id, config);
        }
    }

    private final String id;
    private final Map<String, Object> config;

    public PipelineConfiguration(String id, Map<String, Object> config) {
        this.id = Objects.requireNonNull(id);
        this.config = deepCopy(config, true); // defensive deep copy
    }

    /**
     * A convenience constructor that parses some bytes as a map representing a pipeline's config and then delegates to the
     * conventional {@link #PipelineConfiguration(String, Map)} constructor.
     *
     * @param id the id of the pipeline
     * @param config a parse-able bytes reference that will return a pipeline configuration
     * @param xContentType the content-type to use while parsing the pipeline configuration
     */
    public PipelineConfiguration(String id, BytesReference config, XContentType xContentType) {
        this(id, XContentHelper.convertToMap(config, true, xContentType).v2());
    }

    public String getId() {
        return id;
    }

    /**
     * @return a reference to the unmodifiable configuration map for this pipeline
     */
    public Map<String, Object> getConfig() {
        return getConfig(true);
    }

    /**
     * @param unmodifiable whether the returned map should be unmodifiable or not
     * @return a reference to the unmodifiable config map (if unmodifiable is true) or
     * a reference to a freshly-created mutable deep copy of the config map (if unmodifiable is false)
     */
    public Map<String, Object> getConfig(boolean unmodifiable) {
        if (unmodifiable) {
            return config; // already unmodifiable
        } else {
            return deepCopy(config, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deepCopy(final T value, final boolean unmodifiable) {
        return (T) innerDeepCopy(value, unmodifiable);
    }

    private static Object innerDeepCopy(final Object value, final boolean unmodifiable) {
        if (value instanceof Map<?, ?> mapValue) {
            final Map<Object, Object> copy = Maps.newLinkedHashMapWithExpectedSize(mapValue.size()); // n.b. maintain ordering
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(innerDeepCopy(entry.getKey(), unmodifiable), innerDeepCopy(entry.getValue(), unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableMap(copy) : copy;
        } else if (value instanceof List<?> listValue) {
            final List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(innerDeepCopy(itemValue, unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableList(copy) : copy;
        } else {
            // if this list of expected value types ends up not being exhaustive, then we want to learn about that
            // at development time, but it's probably better to err on the side of passing through the value at runtime
            assert (value == null || value instanceof String || value instanceof Number || value instanceof Boolean)
                : "unexpected value type [" + value.getClass() + "]";
            return value;
        }
    }

    public Integer getVersion() {
        Object o = config.get("version");
        if (o == null) {
            return null;
        } else if (o instanceof Number number) {
            return number.intValue();
        } else {
            throw new IllegalStateException("unexpected version type [" + o.getClass().getName() + "]");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("config", config);
        builder.endObject();
        return builder;
    }

    public static PipelineConfiguration readFrom(StreamInput in) throws IOException {
        final String id = in.readString();
        final Map<String, Object> config;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
            config = in.readGenericMap();
        } else {
            final BytesReference bytes = in.readSlicedBytesReference();
            final XContentType type = in.readEnum(XContentType.class);
            config = XContentHelper.convertToMap(bytes, true, type).v2();
        }
        return new PipelineConfiguration(id, config);
    }

    public static Diff<PipelineConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(PipelineConfiguration::readFrom, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
            out.writeGenericMap(config);
        } else {
            XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).prettyPrint();
            builder.map(config);
            out.writeBytesReference(BytesReference.bytes(builder));
            XContentHelper.writeTo(out, XContentType.JSON);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PipelineConfiguration that = (PipelineConfiguration) o;

        if (id.equals(that.id) == false) return false;
        return config.equals(that.config);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + config.hashCode();
        return result;
    }

    /**
     * Returns a copy of this object with processor upgrades applied, if necessary. Otherwise, returns this object.
     *
     * <p>The given upgrader is applied to the config map for any processor of the given type.
     */
    PipelineConfiguration maybeUpgradeProcessors(String type, IngestMetadata.ProcessorConfigUpgrader upgrader) {
        Map<String, Object> mutableConfigMap = getConfig(false);
        boolean changed = false;
        // This should be a List of Maps, where the keys are processor types and the values are config maps.
        // But we'll skip upgrading rather than fail if not.
        if (mutableConfigMap.get(Pipeline.PROCESSORS_KEY) instanceof Iterable<?> processors) {
            for (Object processor : processors) {
                if (processor instanceof Map<?, ?> processorMap && processorMap.get(type) instanceof Map<?, ?> targetProcessor) {
                    @SuppressWarnings("unchecked") // All XContent maps will be <String, Object>
                    Map<String, Object> processorConfigMap = (Map<String, Object>) targetProcessor;
                    if (upgrader.maybeUpgrade(processorConfigMap)) {
                        changed = true;
                    }
                }
            }
        }
        if (changed) {
            return new PipelineConfiguration(id, mutableConfigMap);
        } else {
            return this;
        }
    }
}
