/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;

public class ProjectMetadata implements Iterable<IndexMetadata> {

    final ImmutableOpenMap<String, IndexMetadata> indices;
    final ImmutableOpenMap<String, Set<Index>> aliasedIndices;
    final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    final ImmutableOpenMap<String, Metadata.Custom> customs;

    final int totalNumberOfShards;
    final int totalOpenIndexShards;

    final String[] allIndices;
    final String[] visibleIndices;
    final String[] allOpenIndices;
    final String[] visibleOpenIndices;
    final String[] allClosedIndices;
    final String[] visibleClosedIndices;

    volatile SortedMap<String, IndexAbstraction> indicesLookup;
    final Map<String, MappingMetadata> mappingsByHash;

    final IndexVersion oldestIndexVersion;

    public ProjectMetadata(
        ImmutableOpenMap<String, IndexMetadata> indices,
        ImmutableOpenMap<String, Set<Index>> aliasedIndices,
        ImmutableOpenMap<String, IndexTemplateMetadata> templates,
        ImmutableOpenMap<String, Metadata.Custom> customs,
        int totalNumberOfShards,
        int totalOpenIndexShards,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, MappingMetadata> mappingsByHash,
        IndexVersion oldestIndexVersion
    ) {
        this.indices = indices;
        this.aliasedIndices = aliasedIndices;
        this.templates = templates;
        this.customs = customs;
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;
        this.allIndices = allIndices;
        this.visibleIndices = visibleIndices;
        this.allOpenIndices = allOpenIndices;
        this.visibleOpenIndices = visibleOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.visibleClosedIndices = visibleClosedIndices;
        this.indicesLookup = indicesLookup;
        this.mappingsByHash = mappingsByHash;
        this.oldestIndexVersion = oldestIndexVersion;
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.values().iterator();
    }

    public Stream<IndexMetadata> stream() {
        return indices.values().stream();
    }

    public int size() {
        return indices.size();
    }

}
