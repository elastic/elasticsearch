/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Retrieves the individual node checks and reduces them to a list of deprecation warnings
 */
public class NodeDeprecationChecker {

    private static final Logger logger = LogManager.getLogger(NodeDeprecationChecker.class);
    private final ThreadPool threadPool;

    public NodeDeprecationChecker(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void check(Client client, ActionListener<List<DeprecationIssue>> listener) {
        NodesDeprecationCheckRequest nodeDepReq = new NodesDeprecationCheckRequest("_all");
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.DEPRECATION_ORIGIN,
            NodesDeprecationCheckAction.INSTANCE,
            nodeDepReq,
            new ThreadedActionListener<>(threadPool.generic(), listener.delegateFailureAndWrap((l, response) -> {
                if (response.hasFailures()) {
                    List<String> failedNodeIds = response.failures()
                        .stream()
                        .map(failure -> failure.nodeId() + ": " + failure.getMessage())
                        .collect(Collectors.toList());
                    logger.warn("nodes failed to run deprecation checks: {}", failedNodeIds);
                    for (FailedNodeException failure : response.failures()) {
                        logger.debug("node {} failed to run deprecation checks: {}", failure.nodeId(), failure);
                    }
                }
                l.onResponse(reduceToDeprecationIssues(response));
            }))
        );
    }

    /**
     * This method rolls up DeprecationIssues that are identical but on different nodes. It also rolls up DeprecationIssues that are
     * identical (and on different nodes) except that they differ in the removable settings listed in their meta object. We roll these up
     * by taking the intersection of all removable settings in otherwise identical DeprecationIssues. That way we don't claim that a
     * setting can be automatically removed if any node has it in its elasticsearch.yml.
     * @param response the response that contains the deprecation issues of single nodes
     * @return a list of deprecation issues grouped accordingly.
     */
    static List<DeprecationIssue> reduceToDeprecationIssues(NodesDeprecationCheckResponse response) {
        // A collection whose values are lists of DeprecationIssues that differ only by meta values (if that):
        Collection<List<Tuple<DeprecationIssue, String>>> issuesToMerge = getDeprecationIssuesThatDifferOnlyByMeta(response.getNodes());
        // A map of DeprecationIssues (containing only the intersection of removable settings) to the nodes they are seen on
        Map<DeprecationIssue, List<String>> issueToListOfNodesMap = getMergedIssuesToNodesMap(issuesToMerge);

        return issueToListOfNodesMap.entrySet().stream().map(entry -> {
            DeprecationIssue issue = entry.getKey();
            String details = issue.getDetails() != null ? issue.getDetails() + " " : "";
            return new DeprecationIssue(
                issue.getLevel(),
                issue.getMessage(),
                issue.getUrl(),
                details + "(nodes impacted: " + entry.getValue() + ")",
                issue.isResolveDuringRollingUpgrade(),
                issue.getMeta()
            );
        }).collect(Collectors.toList());
    }

    /*
     * This method pulls all the DeprecationIssues from the given nodeResponses, and buckets them into lists of DeprecationIssues that
     * differ at most by meta values (if that). The returned tuples also contain the node name the deprecation issue was found on. If all
     * nodes in the cluster were configured identically then all tuples in a list will differ only by the node name.
     */
    private static Collection<List<Tuple<DeprecationIssue, String>>> getDeprecationIssuesThatDifferOnlyByMeta(
        List<NodesDeprecationCheckAction.NodeResponse> nodeResponses
    ) {
        Map<DeprecationIssue, List<Tuple<DeprecationIssue, String>>> issuesToMerge = new HashMap<>();
        for (NodesDeprecationCheckAction.NodeResponse resp : nodeResponses) {
            for (DeprecationIssue issue : resp.getDeprecationIssues()) {
                issuesToMerge.computeIfAbsent(
                    new DeprecationIssue(
                        issue.getLevel(),
                        issue.getMessage(),
                        issue.getUrl(),
                        issue.getDetails(),
                        issue.isResolveDuringRollingUpgrade(),
                        null // Intentionally removing meta from the key so that it's not taken into account for equality
                    ),
                    (key) -> new ArrayList<>()
                ).add(new Tuple<>(issue, resp.getNode().getName()));
            }
        }
        return issuesToMerge.values();
    }

    /*
     * At this point we have one DeprecationIssue per node for a given deprecation. This method rolls them up into a single DeprecationIssue
     * with a list of nodes that they appear on. If two DeprecationIssues on two different nodes differ only by the set of removable
     * settings (i.e. they have different elasticsearch.yml configurations) then this method takes the intersection of those settings when
     * it rolls them up.
     */
    private static Map<DeprecationIssue, List<String>> getMergedIssuesToNodesMap(
        Collection<List<Tuple<DeprecationIssue, String>>> issuesToMerge
    ) {
        Map<DeprecationIssue, List<String>> issueToListOfNodesMap = new HashMap<>();
        for (List<Tuple<DeprecationIssue, String>> similarIssues : issuesToMerge) {
            DeprecationIssue leastCommonDenominator = DeprecationIssue.getIntersectionOfRemovableSettings(
                similarIssues.stream().map(Tuple::v1).toList()
            );
            issueToListOfNodesMap.computeIfAbsent(leastCommonDenominator, (key) -> new ArrayList<>())
                .addAll(similarIssues.stream().map(Tuple::v2).toList());
        }
        return issueToListOfNodesMap;
    }

}
