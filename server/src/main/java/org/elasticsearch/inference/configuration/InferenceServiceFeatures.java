/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The set of {@link InferenceFeature}s a single inference service reports, keyed by feature name:
 * <pre>{@code
 * {
 *   "non_streaming_chat": { "supported": true }
 * }
 * }</pre>
 * <p>
 * Services declare only the features that apply to them, so two services can report disjoint sets. Each entry is
 * resolved polymorphically — by {@link NamedXContentRegistry} when parsing and by {@link NamedWriteableRegistry} on the
 * transport layer — which is why this class owns both registries. Registering a new feature is a one-line addition to
 * {@link #getNamedWriteables()} and {@link #getNamedXContentEntries()}; nothing else needs to change.
 */
public class InferenceServiceFeatures implements Writeable, ToXContentObject {

    /**
     * Every field inside this object is a feature name, so unknown fields are resolved as named objects rather than
     * being declared up front. An unregistered name fails the parse, which means adding a feature is a breaking change
     * for anything parsing this with an older registry. That is the right trade today because the only parsers are
     * ours; revisit it if a client ever reads this response back.
     */
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
        "inference_features",
        InferenceFeature.class,
        Builder::add,
        Builder::new
    );

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(InferenceFeature.class, NonStreamingChatFeature.NAME, NonStreamingChatFeature::new)
        );
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentEntries() {
        return List.of(
            new NamedXContentRegistry.Entry(
                InferenceFeature.class,
                new ParseField(NonStreamingChatFeature.NAME),
                (p, c) -> NonStreamingChatFeature.fromXContent(p)
            )
        );
    }

    /**
     * Used by {@code InferenceServiceConfiguration#fromXContentBytes} so that parsing works without the caller having
     * to thread a registry through. Parsers built elsewhere must supply this registry themselves.
     */
    public static final NamedXContentRegistry NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(getNamedXContentEntries());

    public static InferenceServiceFeatures of(InferenceFeature... features) {
        var builder = new Builder();
        for (var feature : features) {
            builder.add(feature);
        }

        return builder.build();
    }

    public static InferenceServiceFeatures fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    public static class Builder {
        private final Map<String, InferenceFeature> features = new TreeMap<>();

        public Builder add(InferenceFeature feature) {
            features.put(feature.getWriteableName(), feature);
            return this;
        }

        public InferenceServiceFeatures build() {
            return new InferenceServiceFeatures(features);
        }
    }

    private final Map<String, InferenceFeature> features;

    private InferenceServiceFeatures(Map<String, InferenceFeature> features) {
        // A sorted map keeps the rendered field order stable regardless of how the features were added.
        this.features = Collections.unmodifiableMap(new TreeMap<>(features));
    }

    public InferenceServiceFeatures(StreamInput in) throws IOException {
        var featureMap = new TreeMap<String, InferenceFeature>();
        for (var feature : in.readNamedWriteableCollectionAsList(InferenceFeature.class)) {
            featureMap.put(feature.getWriteableName(), feature);
        }
        this.features = Collections.unmodifiableMap(featureMap);
    }

    public Map<String, InferenceFeature> getFeatures() {
        return features;
    }

    @Nullable
    public InferenceFeature get(String featureName) {
        return features.get(featureName);
    }

    /**
     * Returns true only when the service declared exactly this feature. Equality is the feature's own, so a
     * {@link SupportedInferenceFeature} declared as unsupported does not match its supported counterpart
     */
    public boolean has(InferenceFeature feature) {
        return feature.equals(features.get(feature.getWriteableName()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(features.values());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (var feature : features.entrySet()) {
            builder.field(feature.getKey(), feature.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (InferenceServiceFeatures) o;
        return features.equals(that.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(features);
    }

    @Override
    public String toString() {
        return features.toString();
    }
}
