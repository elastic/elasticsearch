/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.indices.AssociatedIndexDescriptor.buildAutomaton;

/**
 * Describes a {@link DataStream} that is reserved for use by a system feature.
 *
 * <p>A system data stream is managed by the system and protected by the system against user modifications so that system features are
 * not broken by inadvertent user operations.
 *
 * <p>Unlike a {@link SystemIndexDescriptor}, a SystemDataStreamDescriptor does not take an index pattern. Rather, it derives the expected
 * backing index pattern from the data stream name.
 *
 * <p>A SystemDataStreamDescriptor defines a list of allowed product origins. If that list is empty, only system operations can read,
 * write to, or modify the system data stream. If there are entries in the list, then a special request header must be used in any
 * request that accesses the system data stream, either through plugin-provided APIs or through data stream APIs.
 *
 * <p>A SystemDataStreamDescriptor may be internal or external. If internal, the system feature must define APIs for interacting with
 * the system data stream. If external, the system feature will allow use of the data stream API, assuming the correct permissions
 * and product origin flag.
 *
 * <p>One interesting implementation detail is that the SystemDataStreamDescriptor manages its own templates for the data stream, so
 * although they have names, they are never listed in the index template or component template APIs.
 *
 * <p>The descriptor also provides names for the thread pools that Elasticsearch should use to read, search, or modify the descriptor’s
 * indices.
 */
public class SystemDataStreamDescriptor {

    private final String dataStreamName;
    private final String description;
    private final Type type;
    private final ComposableIndexTemplate composableIndexTemplate;
    private final Map<String, ComponentTemplate> componentTemplates;
    private final List<String> allowedElasticProductOrigins;
    private final ExecutorNames executorNames;
    private final CharacterRunAutomaton characterRunAutomaton;

    /**
     * Creates a new descriptor for a system data descriptor
     * @param dataStreamName the name of the data stream. Must not be {@code null}
     * @param description a brief description of what the data stream is used for. Must not be {@code null}
     * @param type the {@link Type} of the data stream which determines how the data stream can be accessed. Must not be {@code null}
     * @param composableIndexTemplate the {@link ComposableIndexTemplate} that contains the mappings and settings for the data stream.
     *                                Must not be {@code null}
     * @param componentTemplates a map that contains {@link ComponentTemplate} instances corresponding to those references in the
     *                           {@link ComposableIndexTemplate}
     * @param allowedElasticProductOrigins a list of product origin values that are allowed to access this data stream if the
     *                                     type is {@link Type#EXTERNAL}. Must not be {@code null}
     * @param executorNames thread pools that should be used for operations on the system data stream
     */
    public SystemDataStreamDescriptor(
        String dataStreamName,
        String description,
        Type type,
        ComposableIndexTemplate composableIndexTemplate,
        Map<String, ComponentTemplate> componentTemplates,
        List<String> allowedElasticProductOrigins,
        ExecutorNames executorNames
    ) {
        this.dataStreamName = Objects.requireNonNull(dataStreamName, "dataStreamName must be specified");
        if (dataStreamName.length() < 2) {
            throw new IllegalArgumentException("system data stream name [" + dataStreamName + "] but must at least 2 characters in length");
        }
        if (dataStreamName.charAt(0) != '.') {
            throw new IllegalArgumentException("system data stream name [" + dataStreamName + "] but must start with the character [.]");
        }
        this.description = Objects.requireNonNull(description, "description must be specified");
        this.type = Objects.requireNonNull(type, "type must be specified");
        this.composableIndexTemplate = Objects.requireNonNull(composableIndexTemplate, "composableIndexTemplate must be provided");
        this.componentTemplates = componentTemplates == null ? Map.of() : Map.copyOf(componentTemplates);
        this.allowedElasticProductOrigins = Objects.requireNonNull(
            allowedElasticProductOrigins,
            "allowedElasticProductOrigins must not be null"
        );
        if (type == Type.EXTERNAL && allowedElasticProductOrigins.isEmpty()) {
            throw new IllegalArgumentException("External system data stream without allowed products is not a valid combination");
        }
        this.executorNames = Objects.nonNull(executorNames) ? executorNames : ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS;

        this.characterRunAutomaton = new CharacterRunAutomaton(buildAutomaton(backingIndexPatternForDataStream(this.dataStreamName)));
    }

    public String getDataStreamName() {
        return dataStreamName;
    }

    /**
     * Retrieve backing indices for this system data stream
     * @param metadata Metadata in which to look for indices
     * @return List of names of backing indices
     */
    public List<String> getBackingIndexNames(Metadata metadata) {
        return metadata.indices().keySet().stream().filter(this.characterRunAutomaton::run).toList();
    }

    public String getDescription() {
        return description;
    }

    public ComposableIndexTemplate getComposableIndexTemplate() {
        return composableIndexTemplate;
    }

    public boolean isExternal() {
        return type == Type.EXTERNAL;
    }

    public String getBackingIndexPattern() {
        return backingIndexPatternForDataStream(getDataStreamName());
    }

    private static String backingIndexPatternForDataStream(String dataStream) {
        return DataStream.BACKING_INDEX_PREFIX + dataStream + "-*";
    }

    public List<String> getAllowedElasticProductOrigins() {
        return allowedElasticProductOrigins;
    }

    public Map<String, ComponentTemplate> getComponentTemplates() {
        return componentTemplates;
    }

    /**
     * Get the names of the thread pools that should be used for operations on this data stream.
     * @return Names for get, search, and write executors.
     */
    public ExecutorNames getThreadPoolNames() {
        return this.executorNames;
    }

    public enum Type {
        INTERNAL,
        EXTERNAL
    }
}
