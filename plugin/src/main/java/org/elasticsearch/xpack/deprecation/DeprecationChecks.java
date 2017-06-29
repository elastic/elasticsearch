/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    private DeprecationChecks() {
    }

    static List<BiFunction<List<NodeInfo>, ClusterState, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            // STUB: TODO(talevy): add checks
        ));

    static List<BiFunction<List<NodeInfo>, ClusterState, DeprecationIssue>> NODE_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            // STUB: TODO(talevy): add checks
        ));

    @SuppressWarnings("unchecked")
    static List<Function<IndexMetaData, DeprecationIssue>> INDEX_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            indexMetaData -> {
                List<String> issues = new ArrayList<>();
                if (indexMetaData.getCreationVersion().onOrBefore(Version.V_5_6_0)) {
                    for (ObjectCursor<MappingMetaData> mappingMetaData : indexMetaData.getMappings().values()) {
                        Map<String, Object> sourceAsMap = mappingMetaData.value.sourceAsMap();
                        ((Map<String, Object>) sourceAsMap.getOrDefault("properties", Collections.emptyMap()))
                            .forEach((key, value) -> {
                                Map<String, Object> valueMap = ((Map<String, Object>) value);
                                if ("boolean".equals(valueMap.get("type"))) {
                                    issues.add("type: " + mappingMetaData.value.type() + ", field: " + key);
                                }
                            });
                    }
                }
                return new DeprecationIssue(DeprecationIssue.Level.INFO, "Coercion of boolean fields",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                        "breaking_60_mappings_changes.html#_coercion_of_boolean_fields",
                    Arrays.toString(issues.toArray()));
            }
        ));

    /**
     * helper utility function to reduce repeat of running a specific {@link Set} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (BiFunction, Function, including the appropriate arguments)
     * @return The list of {@link DeprecationIssue} that were found in the cluster
     */
    static <T> List<DeprecationIssue> filterChecks(List<T> checks, Function<T, DeprecationIssue> mapper) {
        return checks.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
    }
}
