/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.InternalTestCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Reads live index state from an {@link InternalTestCluster} for a keyword duel. The DSL and ES|QL duel bases
 * extend different cluster superclasses, so this shared accessor keeps the settings and doc-values format
 * lookups, and the started-shard handling they need, in one place.
 */
public final class DuelIndexAccess {

    private DuelIndexAccess() {}

    /**
     * @return the {@link IndexSettings} of {@code index}, read from any node hosting it.
     */
    public static IndexSettings indexSettings(final InternalTestCluster cluster, final Index index) {
        for (final IndicesService indicesService : cluster.getInstances(IndicesService.class)) {
            final IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                return indexService.getIndexSettings();
            }
        }
        throw new AssertionError("no IndexService found for index [" + index.getName() + "]");
    }

    /**
     * @return the distinct per-field doc-values format names recorded for {@code field} across every leaf of
     *         every started shard copy of {@code index}. A well-formed index yields a single name; more than one
     *         means segments disagree on the codec. The set is empty when the field has no doc-values segment. A
     *         copy that is still recovering (for example a replica) is skipped so the read never hits a shard
     *         that cannot serve a searcher.
     */
    public static Set<String> docValuesFormats(final InternalTestCluster cluster, final Index index, final String field) {
        final Set<String> formats = new HashSet<>();
        boolean startedShardSeen = false;
        for (final IndicesService indicesService : cluster.getInstances(IndicesService.class)) {
            final IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                continue;
            }
            final IndexShard shard = indexService.getShardOrNull(0);
            if (shard == null || shard.state() != IndexShardState.STARTED) {
                continue;
            }
            startedShardSeen = true;
            try (Engine.Searcher searcher = shard.acquireSearcher("duel-format-check")) {
                for (final LeafReaderContext leaf : searcher.getLeafContexts()) {
                    final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
                    if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                        formats.add(fieldInfo.getAttribute("PerFieldDocValuesFormat.format"));
                    }
                }
            }
        }
        if (startedShardSeen == false) {
            throw new AssertionError("no started shard found for index [" + index.getName() + "]");
        }
        return formats;
    }
}
