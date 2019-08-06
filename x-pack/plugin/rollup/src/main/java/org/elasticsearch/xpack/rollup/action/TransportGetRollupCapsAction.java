/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TransportGetRollupCapsAction extends HandledTransportAction<GetRollupCapsAction.Request, GetRollupCapsAction.Response> {

    private final ClusterService clusterService;

    @Inject
    public TransportGetRollupCapsAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
        super(GetRollupCapsAction.NAME, transportService, actionFilters, GetRollupCapsAction.Request::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetRollupCapsAction.Request request, ActionListener<GetRollupCapsAction.Response> listener) {
        Map<String, RollableIndexCaps> allCaps = getCaps(request.getIndexPattern(), clusterService.state().getMetaData().indices());
        listener.onResponse(new GetRollupCapsAction.Response(allCaps));
    }

    static Map<String, RollableIndexCaps> getCaps(String indexPattern, ImmutableOpenMap<String, IndexMetaData> indices) {
        Map<String, List<RollupJobCaps> > allCaps = new TreeMap<>();
        for (ObjectObjectCursor<String, IndexMetaData> entry : indices) {

            // Does this index have rollup metadata?
            TransportGetRollupCapsAction.findRollupIndexCaps(entry.key, entry.value).ifPresent(cap -> {

                List<RollupJobCaps> jobCaps;
                if (indexPattern.equals(MetaData.ALL)) {
                    // This index has rollup metadata, and since we want _all, just process all of them
                    jobCaps = cap.getJobCaps();
                } else {
                    // This index has rollup metadata, but is it for the index pattern that we're looking for?
                    jobCaps = cap.getJobCapsByIndexPattern(indexPattern);
                }

                jobCaps.forEach(jobCap -> {
                    String pattern = indexPattern.equals(MetaData.ALL)
                        ? jobCap.getIndexPattern() : indexPattern;

                    // Do we already have an entry for this index pattern?
                    List<RollupJobCaps>  indexCaps = allCaps.get(pattern);
                    if (indexCaps == null) {
                        indexCaps = new ArrayList<>();
                    }
                    indexCaps.add(jobCap);
                    allCaps.put(pattern, indexCaps);
                });
            });
        }

        // Convert the mutable lists into the RollableIndexCaps
        return allCaps.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> new RollableIndexCaps(e.getKey(), e.getValue())));
    }

    static Optional<RollupIndexCaps> findRollupIndexCaps(String indexName, IndexMetaData indexMetaData) {
        if (indexMetaData == null) {
            return Optional.empty();
        }

        MappingMetaData rollupMapping = indexMetaData.getMappings().get(RollupField.TYPE_NAME);
        if (rollupMapping == null) {
            return Optional.empty();
        }

        Object objMeta = rollupMapping.getSourceAsMap().get("_meta");
        if (objMeta == null) {
            return Optional.empty();
        }

        RollupIndexCaps caps = RollupIndexCaps.parseMetadataXContent(
            new BytesArray(rollupMapping.source().uncompressed()), indexName);

        if (caps.hasCaps()) {
            return Optional.of(caps);
        }
        return Optional.empty();
    }
}
