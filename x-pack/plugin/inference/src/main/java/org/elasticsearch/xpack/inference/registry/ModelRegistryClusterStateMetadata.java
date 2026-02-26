/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.InferenceIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Custom {@link Metadata} implementation for storing the {@link MinimalServiceSettings} of all models in the {@link ModelRegistry}.
 * Deleted models are retained as tombstones until the {@link ModelRegistry} upgrades from the existing inference index.
 * After the upgrade, all active models are registered.
 */
public class ModelRegistryClusterStateMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "model_registry";

    public static final ModelRegistryClusterStateMetadata EMPTY_NOT_UPGRADED = new ModelRegistryClusterStateMetadata(
        ImmutableOpenMap.of(),
        Set.of()
    );
    public static final ModelRegistryClusterStateMetadata EMPTY_UPGRADED = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.of());

    private static final ParseField UPGRADED_FIELD = new ParseField("upgraded");
    private static final ParseField MODELS_FIELD = new ParseField("models");
    private static final ParseField TOMBSTONES_FIELD = new ParseField("tombstones");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ModelRegistryClusterStateMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        args -> {
            var isUpgraded = (boolean) args[0];
            var settingsMap = (ImmutableOpenMap<String, MinimalServiceSettings>) args[1];
            var deletedIDs = (List<String>) args[2];
            if (isUpgraded) {
                return new ModelRegistryClusterStateMetadata(settingsMap);
            }
            return new ModelRegistryClusterStateMetadata(settingsMap, new HashSet<>(deletedIDs));
        }
    );

    static {
        PARSER.declareBoolean(constructorArg(), UPGRADED_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> {
            ImmutableOpenMap.Builder<String, MinimalServiceSettings> modelMap = ImmutableOpenMap.builder();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                modelMap.put(name, MinimalServiceSettings.parse(p));
            }
            return modelMap.build();
        }, MODELS_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), TOMBSTONES_FIELD);
    }

    public static ModelRegistryClusterStateMetadata fromState(ProjectMetadata projectMetadata) {
        ModelRegistryClusterStateMetadata resp = projectMetadata.custom(TYPE);
        return resp != null ? resp : EMPTY_NOT_UPGRADED;
    }

    private static final TransportVersion INFERENCE_MODEL_REGISTRY_METADATA = TransportVersion.fromName(
        "inference_model_registry_metadata"
    );

    public ModelRegistryClusterStateMetadata withAddedModel(String inferenceEntityId, MinimalServiceSettings settings) {
        return withAddedModels(List.of(new ModelRegistryMetadataTask.ModelAndSettings(inferenceEntityId, settings)));
    }

    public ModelRegistryClusterStateMetadata withAddedModels(List<ModelRegistryMetadataTask.ModelAndSettings> models) {
        var modifiedMap = false;
        ImmutableOpenMap.Builder<String, MinimalServiceSettings> settingsBuilder = ImmutableOpenMap.builder(modelMap);

        for (var model : models) {
            if (model.settings().equals(modelMap.get(model.inferenceEntityId())) == false) {
                modifiedMap = true;

                settingsBuilder.fPut(model.inferenceEntityId(), model.settings());
            }
        }

        if (modifiedMap == false) {
            return this;
        }

        if (isUpgraded) {
            return new ModelRegistryClusterStateMetadata(settingsBuilder.build());
        }

        var newTombstone = new HashSet<>(tombstones);
        models.forEach(existing -> newTombstone.remove(existing.inferenceEntityId()));
        return new ModelRegistryClusterStateMetadata(settingsBuilder.build(), newTombstone);
    }

    public ModelRegistryClusterStateMetadata withRemovedModel(Set<String> inferenceEntityIds) {
        var mapBuilder = ImmutableOpenMap.builder(modelMap);
        for (var toDelete : inferenceEntityIds) {
            mapBuilder.remove(toDelete);
        }
        if (isUpgraded) {
            return new ModelRegistryClusterStateMetadata(mapBuilder.build());
        }

        var newTombstone = new HashSet<>(tombstones);
        newTombstone.addAll(inferenceEntityIds);
        return new ModelRegistryClusterStateMetadata(mapBuilder.build(), newTombstone);
    }

    public ModelRegistryClusterStateMetadata withUpgradedModels(Map<String, MinimalServiceSettings> indexModels) {
        if (isUpgraded) {
            throw new IllegalArgumentException("Already upgraded");
        }
        ImmutableOpenMap.Builder<String, MinimalServiceSettings> builder = ImmutableOpenMap.builder(modelMap);
        for (var entry : indexModels.entrySet()) {
            if (builder.containsKey(entry.getKey()) == false && tombstones.contains(entry.getKey()) == false) {
                builder.fPut(entry.getKey(), entry.getValue());
            }
        }
        return new ModelRegistryClusterStateMetadata(builder.build());
    }

    private final boolean isUpgraded;
    private final ImmutableOpenMap<String, MinimalServiceSettings> modelMap;
    private final Map<String, Set<String>> serviceToInferenceEndpointIds;
    private final Set<String> tombstones;

    public ModelRegistryClusterStateMetadata(ImmutableOpenMap<String, MinimalServiceSettings> modelMap) {
        this(modelMap, null, true);
    }

    public ModelRegistryClusterStateMetadata(ImmutableOpenMap<String, MinimalServiceSettings> modelMap, Set<String> tombstone) {
        this(modelMap, Collections.unmodifiableSet(tombstone), false);
    }

    public ModelRegistryClusterStateMetadata(StreamInput in) throws IOException {
        this.isUpgraded = in.readBoolean();
        this.modelMap = in.readImmutableOpenMap(StreamInput::readString, MinimalServiceSettings::new);
        this.tombstones = isUpgraded ? null : in.readCollectionAsSet(StreamInput::readString);
        this.serviceToInferenceEndpointIds = buildServiceToInferenceEndpointIdsMap(modelMap);
    }

    private ModelRegistryClusterStateMetadata(
        ImmutableOpenMap<String, MinimalServiceSettings> modelMap,
        Set<String> tombstones,
        boolean isUpgraded
    ) {
        this.isUpgraded = isUpgraded;
        this.modelMap = modelMap;
        this.tombstones = tombstones;
        this.serviceToInferenceEndpointIds = buildServiceToInferenceEndpointIdsMap(modelMap);
    }

    private static Map<String, Set<String>> buildServiceToInferenceEndpointIdsMap(
        ImmutableOpenMap<String, MinimalServiceSettings> modelMap
    ) {
        var serviceToInferenceIds = new HashMap<String, Set<String>>();
        for (var entry : modelMap.entrySet()) {
            var settings = entry.getValue();
            var serviceName = settings.service();

            var existingSet = serviceToInferenceIds.get(serviceName);
            if (existingSet == null) {
                existingSet = new HashSet<>();
            }

            existingSet.add(entry.getKey());
            serviceToInferenceIds.put(serviceName, existingSet);
        }
        return serviceToInferenceIds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isUpgraded);
        out.writeMap(modelMap, StreamOutput::writeWriteable);
        if (isUpgraded == false) {
            out.writeStringCollection(tombstones);
        }
    }

    public static ModelRegistryClusterStateMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((b, p) -> b.field(UPGRADED_FIELD.getPreferredName(), isUpgraded)),
            ChunkedToXContentHelper.object(
                MODELS_FIELD.getPreferredName(),
                modelMap,
                e -> (b, p) -> e.getValue().toXContent(b.field(e.getKey()), p)
            ),
            isUpgraded
                ? Collections.emptyIterator()
                : ChunkedToXContentHelper.array(
                    TOMBSTONES_FIELD.getPreferredName(),
                    Iterators.map(tombstones.iterator(), e -> (b, p) -> b.value(e))
                )
        );
    }

    /**
     * Determines whether all models created prior to {@link #INFERENCE_MODEL_REGISTRY_METADATA}
     * have been successfully restored from the {@link InferenceIndex}.
     *
     * @return true if all such models have been restored; false otherwise.
     *
     * If this method returns false, it indicates that there may still be models in the {@link InferenceIndex}
     * that have not yet been referenced in the {@link #getModelMap()}.
     */
    public boolean isUpgraded() {
        return isUpgraded;
    }

    /**
     * Returns all the registered models.
     */
    public ImmutableOpenMap<String, MinimalServiceSettings> getModelMap() {
        return modelMap;
    }

    /**
     * Returns all inference entity IDs for a given service.
     */
    public Set<String> getServiceInferenceIds(String service) {
        if (serviceToInferenceEndpointIds.containsKey(service) == false) {
            return Set.of();
        }

        return Set.copyOf(serviceToInferenceEndpointIds.get(service));
    }

    public MinimalServiceSettings getMinimalServiceSettings(String inferenceEntityId) {
        return modelMap.get(inferenceEntityId);
    }

    public Set<String> getInferenceIds() {
        return Set.copyOf(modelMap.keySet());
    }

    @Override
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom before) {
        return new ModelRegistryMetadataDiff((ModelRegistryClusterStateMetadata) before, this);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return new ModelRegistryMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public boolean isRestorable() {
        // this metadata is created automatically from the inference index if it doesn't exist.
        return false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_MODEL_REGISTRY_METADATA;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return shouldSerialize(version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.modelMap, this.tombstones, this.isUpgraded);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ModelRegistryClusterStateMetadata other = (ModelRegistryClusterStateMetadata) obj;
        return Objects.equals(this.modelMap, other.modelMap)
            && isUpgraded == other.isUpgraded
            && Objects.equals(this.tombstones, other.tombstones);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public Collection<String> getTombstones() {
        return tombstones;
    }

    static class ModelRegistryMetadataDiff implements NamedDiff<Metadata.ProjectCustom> {

        private static final DiffableUtils.DiffableValueReader<String, MinimalServiceSettings> SETTINGS_DIFF_READER =
            new DiffableUtils.DiffableValueReader<>(MinimalServiceSettings::new, MinimalServiceSettings::readDiffFrom);

        final boolean isUpgraded;
        final DiffableUtils.MapDiff<String, MinimalServiceSettings, ImmutableOpenMap<String, MinimalServiceSettings>> settingsDiff;
        final Set<String> tombstone;

        ModelRegistryMetadataDiff(ModelRegistryClusterStateMetadata before, ModelRegistryClusterStateMetadata after) {
            this.isUpgraded = after.isUpgraded;
            this.settingsDiff = DiffableUtils.diff(before.modelMap, after.modelMap, DiffableUtils.getStringKeySerializer());
            this.tombstone = after.isUpgraded ? null : after.tombstones;
        }

        ModelRegistryMetadataDiff(StreamInput in) throws IOException {
            this.isUpgraded = in.readBoolean();
            this.settingsDiff = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), SETTINGS_DIFF_READER);
            this.tombstone = isUpgraded ? null : in.readCollectionAsSet(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(isUpgraded);
            settingsDiff.writeTo(out);
            if (isUpgraded == false) {
                out.writeStringCollection(tombstone);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return INFERENCE_MODEL_REGISTRY_METADATA;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return shouldSerialize(version);
        }

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            var metadata = (ModelRegistryClusterStateMetadata) part;
            if (isUpgraded) {
                return new ModelRegistryClusterStateMetadata(settingsDiff.apply(metadata.modelMap));
            } else {
                return new ModelRegistryClusterStateMetadata(settingsDiff.apply(metadata.modelMap), tombstone);
            }
        }
    }

    static boolean shouldSerialize(TransportVersion version) {
        return version.supports(INFERENCE_MODEL_REGISTRY_METADATA);
    }
}
