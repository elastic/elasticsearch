/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plugin;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.VersionMismatchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.QlVersionMismatchException;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.function.Consumer;

public final class TransportActionUtils {

    /**
     * Execute a *QL request and re-try it in case the first request failed with a {@code VersionMismatchException}
     *
     * @param clusterService The cluster service instance
     * @param onFailure On-failure handler in case the request doesn't fail with a {@code VersionMismatchException}
     * @param queryRunner *QL query execution code, typically a Plan Executor running the query
     * @param retryRequest Re-trial logic
     * @param log Log4j logger
     */
    public static void executeRequestWithRetryAttempt(
        ClusterService clusterService,
        Consumer<Exception> onFailure,
        Consumer<Consumer<Exception>> queryRunner,
        Consumer<DiscoveryNode> retryRequest,
        Logger log
    ) {

        Holder<Boolean> retrySecondTime = new Holder<Boolean>(false);
        queryRunner.accept(e -> {
            // the search request likely ran on nodes with different versions of ES
            // we will retry on a node with an older version that should generate a backwards compatible _search request
            if (e instanceof SearchPhaseExecutionException && e.getCause() instanceof VersionMismatchException) {
                if (log.isDebugEnabled()) {
                    log.debug("Caught exception type [{}] with cause [{}].", e.getClass().getName(), e.getCause());
                }
                DiscoveryNode candidateNode = null;
                DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
                for (DiscoveryNode node : clusterService.state().nodes()) {
                    if (node != localNode && node.getVersion().before(localNode.getVersion())) {
                        candidateNode = node;
                        break;
                    }
                }
                if (candidateNode != null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "Candidate node to resend the request to: address [{}], id [{}], name [{}], version [{}]",
                            candidateNode.getAddress(),
                            candidateNode.getId(),
                            candidateNode.getName(),
                            candidateNode.getVersion()
                        );
                    }
                    // re-send the request to the older node
                    retryRequest.accept(candidateNode);
                } else {
                    retrySecondTime.set(true);
                }
            } else {
                onFailure.accept(e);
            }
        });
        if (retrySecondTime.get()) {
            if (log.isDebugEnabled()) {
                log.debug("No candidate node found, likely all were upgraded in the meantime. Re-trying the original request.");
            }
            queryRunner.accept(onFailure);
        }
    }

    public static void retryOnNodeWithMatchingVersion(
        ClusterService clusterService,
        QlVersionMismatchException exception,
        Consumer<DiscoveryNode> retryRequest,
        Consumer<Exception> onFailure,
        Logger log
    ) {
        if (log.isDebugEnabled()) {
            log.debug(
                "Version mismatch detected: [{}]. Looking for node with matching version [{}] for retry.",
                exception.getMessage(),
                exception.getInputVersion()
            );
        }

        DiscoveryNode candidateNode = findNodeWithVersionOtherThanLocalNode(clusterService, exception.getInputVersion());

        if (candidateNode != null) {
            if (log.isDebugEnabled()) {
                log.debug(
                    "Candidate node to resend the request to: address [{}], id [{}], name [{}], version [{}]",
                    candidateNode.getAddress(),
                    candidateNode.getId(),
                    candidateNode.getName(),
                    candidateNode.getVersion()
                );
            }

            retryRequest.accept(candidateNode);
        } else {
            if (log.isInfoEnabled()) {
                log.info(
                    "No candidate node with version {} found, likely all were upgraded in the meantime. Failing request.",
                    exception.getInputVersion()
                );
            }
            onFailure.accept(exception);
        }

    }

    private static DiscoveryNode findNodeWithVersionOtherThanLocalNode(ClusterService clusterService, Version expectedVersion) {
        DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
        if (expectedVersion.equals(localNode.getVersion())) {
            throw new QlIllegalArgumentException("Local node is already on expected version {}.", expectedVersion);
        }

        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (node.getVersion().equals(expectedVersion)) {
                return node;
            }
        }
        return null;
    }

}
