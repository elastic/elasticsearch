/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.SerializableStats;

import java.io.IOException;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_ENDPOINT_CACHE_ENABLED;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_ENDPOINT_CACHE_EXPIRY;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_ENDPOINT_CACHE_WEIGHT;

/**
 * A registry that assembles and caches Inference Endpoints, {@link Model}, for reuse.
 * Models are high read and minimally written, where changes only occur during updates and deletes.
 * The cache is invalidated via the {@link ClearInferenceEndpointCacheAction} so that every node gets the invalidation
 * message.
 */
public class InferenceEndpointRegistry {

    private static final Logger log = LogManager.getLogger(InferenceEndpointRegistry.class);
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final ProjectResolver projectResolver;
    private final Cache<InferenceIdAndProject, Model> cache;
    private volatile boolean cacheEnabled;

    public InferenceEndpointRegistry(
        ClusterService clusterService,
        Settings settings,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        ProjectResolver projectResolver
    ) {
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.projectResolver = projectResolver;
        this.cache = CacheBuilder.<InferenceIdAndProject, Model>builder()
            .setMaximumWeight(INFERENCE_ENDPOINT_CACHE_WEIGHT.get(settings))
            .setExpireAfterWrite(INFERENCE_ENDPOINT_CACHE_EXPIRY.get(settings))
            .build();
        this.cacheEnabled = INFERENCE_ENDPOINT_CACHE_ENABLED.get(settings);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(INFERENCE_ENDPOINT_CACHE_ENABLED, enabled -> this.cacheEnabled = enabled);
    }

    public void getEndpoint(String inferenceEntityId, ActionListener<Model> listener) {
        var key = new InferenceIdAndProject(inferenceEntityId, projectResolver.getProjectId());
        var cachedModel = cacheEnabled ? cache.get(key) : null;
        if (cachedModel != null) {
            log.debug("Retrieved [{}] from cache.", inferenceEntityId);
            listener.onResponse(cachedModel);
        } else {
            loadFromIndex(key, listener);
        }
    }

    void invalidateAll(ProjectId projectId) {
        // copy to an interim list because cache.keys() does not allow inline mutations
        if (cacheEnabled) {
            StreamSupport.stream(cache.keys().spliterator(), false)
                .filter(key -> key.projectId.equals(projectId))
                .toList()
                .forEach(cache::invalidate);
        }
    }

    private void loadFromIndex(InferenceIdAndProject idAndProject, ActionListener<Model> listener) {
        modelRegistry.getModelWithSecrets(idAndProject.inferenceEntityId(), listener.delegateFailureAndWrap((l, unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service())
                .orElseThrow(
                    () -> new ResourceNotFoundException(
                        "Unknown service [{}] for model [{}]",
                        unparsedModel.service(),
                        idAndProject.inferenceEntityId()
                    )
                );

            var model = service.parsePersistedConfigWithSecrets(
                unparsedModel.inferenceEntityId(),
                unparsedModel.taskType(),
                unparsedModel.settings(),
                unparsedModel.secrets()
            );

            if (cacheEnabled) {
                cache.put(idAndProject, model);
            }
            l.onResponse(model);
        }));
    }

    public Stats stats() {
        return cacheEnabled ? new Stats(cache.stats()) : Stats.EMPTY;
    }

    public boolean cacheEnabled() {
        return cacheEnabled;
    }

    private record InferenceIdAndProject(String inferenceEntityId, ProjectId projectId) {}

    public record Stats(Cache.Stats stats) implements SerializableStats {
        public static final String NAME = "inference_endpoint_registry_stats";
        private static final String CACHE_HITS = "cache_hits";
        private static final String CACHE_MISSES = "cache_misses";
        private static final String CACHE_EVICTIONS = "cache_evictions";

        private static final Stats EMPTY = new Stats(new Cache.Stats(0, 0, 0));

        public Stats(StreamInput in) throws IOException {
            this(new Cache.Stats(in.readLong(), in.readLong(), in.readLong()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(stats.getHits());
            out.writeLong(stats.getMisses());
            out.writeLong(stats.getEvictions());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(CACHE_HITS, stats.getHits())
                .field(CACHE_MISSES, stats.getMisses())
                .field(CACHE_EVICTIONS, stats.getEvictions())
                .endObject();
        }

        public long hits() {
            return stats.getHits();
        }

        public long misses() {
            return stats.getMisses();
        }

        public long evictions() {
            return stats.getEvictions();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
