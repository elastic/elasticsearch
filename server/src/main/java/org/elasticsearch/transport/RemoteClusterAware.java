/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for services and components that utilize linked projects.
 */
public abstract class RemoteClusterAware implements LinkedProjectConfigService.LinkedProjectConfigListener {
    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    protected final Settings settings;
    private final String nodeName;
    private final boolean isRemoteClusterClientEnabled;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        this.settings = settings;
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.isRemoteClusterClientEnabled = DiscoveryNode.isRemoteClusterClient(settings);
    }

    protected String getNodeName() {
        return nodeName;
    }

    /**
     * Check whether the index expression represents remote index or not.
     * The index name is assumed to be individual index (no commas) but can contain `-`, wildcards,
     * datemath, remote cluster name and any other syntax permissible in index expression component.
     */
    public static boolean isRemoteIndexName(String indexExpression) {
        if (indexExpression.isEmpty() || indexExpression.charAt(0) == '<' || indexExpression.startsWith("-<")) {
            // This is date math, but even if it is not, the remote can't start with '<'.
            // Thus, whatever it is, this is definitely not a remote index.
            return false;
        }
        int idx = indexExpression.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
        // Check to make sure the remote cluster separator ':' isn't actually a selector separator '::'
        boolean isSelector = indexExpression.startsWith(SelectorResolver.SELECTOR_SEPARATOR, idx);
        // Note remote index name also can not start with ':'
        return idx > 0 && isSelector == false;
    }

    /**
     * Extracts the list of remote index expressions from the given array of index expressions
     */
    public static List<String> getRemoteIndexExpressions(String... expressions) {
        List<String> crossClusterIndices = new ArrayList<>();
        for (int i = 0; i < expressions.length; i++) {
            if (isRemoteIndexName(expressions[i])) {
                crossClusterIndices.add(expressions[i]);
            }
        }
        return crossClusterIndices;
    }

    /**
     * Split the index name into remote cluster alias and index name.
     * The index expression is assumed to be individual index (no commas) but can contain `-`, wildcards,
     * datemath, remote cluster name and any other syntax permissible in index expression component.
     * There's no guarantee the components actually represent existing remote cluster or index, only
     * rudimentary checks are done on the syntax.
     */
    public static QualifiedIndexExpression splitIndexName(String indexExpression) {
        if (indexExpression.isEmpty() || indexExpression.charAt(0) == '<' || indexExpression.startsWith("-<")) {
            // This is date math, but even if it is not, the remote can't start with '<'.
            // Thus, whatever it is, this is definitely not a remote index.
            return new QualifiedIndexExpression(null, indexExpression);
        }
        int i = indexExpression.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
        if (i == 0) {
            throw new IllegalArgumentException("index name [" + indexExpression + "] is invalid because the remote part is empty");
        }
        if (i < 0 || indexExpression.startsWith(SelectorResolver.SELECTOR_SEPARATOR, i)) {
            // Either no colon present, or the colon was a part of a selector separator (::)
            return new QualifiedIndexExpression(null, indexExpression);
        } else {
            return new QualifiedIndexExpression(indexExpression.substring(0, i), indexExpression.substring(i + 1));
        }
    }

    /**
     * Result of splitting a qualified index expression into its cluster and index parts.
     * <p>
     * For local indices, {@link #clusterAlias()} is {@code null}. The index part is always
     * an expression (may contain wildcards, date math, selectors, etc.), not necessarily a concrete index name.
     */
    public record QualifiedIndexExpression(@Nullable String clusterAlias, String indexExpression) {

        public QualifiedIndexExpression {
            assert indexExpression != null : "index expression must not be null";
        }

        /**
         * @return clusterAlias or {@code LOCAL_CLUSTER_GROUP_KEY} if index is local
         */
        public String getClusterGroupingKey() {
            return clusterAlias != null ? clusterAlias : LOCAL_CLUSTER_GROUP_KEY;
        }

        @Override
        public String toString() {
            return clusterAlias != null ? clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexExpression : indexExpression;
        }
    }

    /**
     * Groups indices per cluster by splitting remote cluster-alias, index-name pairs on {@link #REMOTE_CLUSTER_INDEX_SEPARATOR}. All
     * indices per cluster are collected as a list in the returned map keyed by the cluster alias. Local indices are grouped under
     * {@link #LOCAL_CLUSTER_GROUP_KEY}. The returned map is mutable.
     *
     * This method supports excluding clusters by using the {@code -cluster:*} index expression.
     * Expressions are processed left to right, and order is significant: a cluster can only be excluded if it has been included
     * by a preceding expression, and including a cluster after it has been excluded adds it back.
     * For example, if requestIndices is [blogs, *:blogs, -remote1:*] and *:blogs resolves to "remote1:blogs, remote2:blogs"
     * the map returned by the function will not have the remote1 entry. It will have only {"":blogs, remote2:blogs}.
     * The index for the excluded cluster must be '*' to clarify that the entire cluster should be removed.
     * A wildcard in the "-" excludes notation is also allowed. For example, suppose there are three remote clusters,
     * remote1, remote2, remote3, and this index expression is provided: blogs,rem*:blogs,-rem*1:*. That would successfully
     * remove remote1 from the list of clusters to be included.
     *
     * This method also supports excluding indices on remote clusters by using {@code -cluster:index} as an alternative
     * form of {@code cluster:-index}. For example, {@code -remote:foo*} is equivalent to {@code remote:-foo*}.
     *
     * @param remoteClusterNames the remote cluster names. If a clusterAlias is preceded by a minus sign that cluster will be excluded.
     * @param requestIndices the indices in the search request to filter
     *
     * @return a map of grouped remote and local indices
     */
    protected Map<String, List<String>> groupClusterIndices(Set<String> remoteClusterNames, String[] requestIndices) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        Set<String> everIncluded = new HashSet<>();
        boolean hasExclusions = false;
        for (String index : requestIndices) {
            // ensure that `index` is a remote name and not a datemath expression which includes ':' symbol
            // Remote names can not start with '<' so we are assuming that if the first character is '<' then it is a datemath expression.
            var split = splitIndexName(index);
            if (split.clusterAlias != null) {
                if (isRemoteClusterClientEnabled == false) {
                    assert remoteClusterNames.isEmpty() : remoteClusterNames;
                    throw new IllegalArgumentException("node [" + nodeName + "] does not have the remote cluster client role enabled");
                }
                String remoteClusterName = split.clusterAlias;
                String indexName = split.indexExpression;
                boolean isNegative = remoteClusterName.startsWith("-");
                List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(
                    remoteClusterNames,
                    isNegative ? remoteClusterName.substring(1) : remoteClusterName
                );
                if (isNegative) {
                    Tuple<String, String> indexAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexName);
                    String indexPart = indexAndSelector.v1();
                    String selectorString = indexAndSelector.v2();
                    hasExclusions = true;
                    if (indexPart.equals("*")) {
                        if (selectorString != null) {
                            throw new IllegalArgumentException(
                                Strings.format(
                                    "To exclude a cluster you must not specify a selector, but found selector: [%s]",
                                    selectorString
                                )
                            );
                        }
                        List<String> excludeFailed = new ArrayList<>();
                        for (String cluster : clusters) {
                            perClusterIndices.remove(cluster);
                            if (everIncluded.contains(cluster) == false) {
                                excludeFailed.add(cluster);
                            }
                        }
                        if (excludeFailed.isEmpty() == false) {
                            throw new IllegalArgumentException(
                                Strings.format(
                                    "Attempt to exclude cluster%s %s failed as %s not included in the list of clusters to be included"
                                        + " (note: the \"include\" expression must precede the \"exclude\" expression)",
                                    excludeFailed.size() == 1 ? "" : "s",
                                    excludeFailed,
                                    excludeFailed.size() == 1 ? "it is" : "they are"
                                )
                            );
                        }
                    } else if (indexPart.startsWith("-")) {
                        throw new IllegalArgumentException(
                            Strings.format(
                                "cannot apply exclusion for both the cluster and the index expression, but found: [%s]",
                                remoteClusterName + ":" + indexPart
                            )
                        );
                    } else {
                        // an index exclusion like -remote:logs is syntactic sugar for including the cluster but excluding the index,
                        // so we need to add the cluster to the list of included clusters
                        assert indexName.startsWith("-") == false : "index name should not start with - but was [" + indexName + "]";
                        everIncluded.addAll(clusters);
                        for (String clusterName : clusters) {
                            perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add("-" + indexName);
                        }
                    }
                } else {
                    everIncluded.addAll(clusters);
                    for (String clusterName : clusters) {
                        perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
                    }
                }
            } else {
                perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
        }
        if (hasExclusions && perClusterIndices.isEmpty()) {
            throw new IllegalArgumentException(
                "The '-' exclusions in the index expression list excludes all indexes. Nothing to search. Input: ["
                    + String.join(",", requestIndices)
                    + "]"
            );
        }
        return perClusterIndices;
    }

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName
            : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
