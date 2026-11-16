/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Optional;

/**
 * Gates shared by the put/update/start/preview datafeed actions for features that need a minimum
 * cluster transport version or are behind {@link MachineLearning#ESQL_DATAFEEDS_FEATURE_FLAG}.
 */
public final class DatafeedEsqlGates {

    private DatafeedEsqlGates() {}

    /**
     * @param minRequired the minimum transport version (and reason) a datafeed config or update requires, if any
     * @return the reason the cluster cannot support the config or update yet, or empty if the minimum transport
     * version is satisfied or not required
     */
    public static Optional<String> unsupportedReason(Optional<Tuple<TransportVersion, String>> minRequired, ClusterState state) {
        if (minRequired.isPresent() && state.getMinTransportVersion().supports(minRequired.get().v1()) == false) {
            return Optional.of(minRequired.get().v2());
        }
        return Optional.empty();
    }

    /**
     * Validates that a datafeed may be created on this cluster: transport version support and ES|QL feature flag when
     * the config uses {@code esql_query}.
     */
    public static void validateDatafeedCreate(DatafeedConfig datafeed, ClusterState state) {
        validateDatafeedCreate(
            datafeed.getId(),
            datafeed.minRequiredTransportVersion(),
            datafeed.getEsqlQuery() != null,
            state,
            MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled()
        );
    }

    /**
     * Same checks as {@link #validateDatafeedCreate(DatafeedConfig, ClusterState)} for an embedded datafeed builder.
     */
    public static void validateDatafeedCreate(
        String datafeedId,
        Optional<Tuple<TransportVersion, String>> minRequired,
        boolean usesEsqlQuery,
        ClusterState state,
        boolean esqlDatafeedsEnabled
    ) {
        validateClusterSupportsMinTransportVersion(datafeedId, minRequired, state);
        if (usesEsqlQuery && esqlDatafeedsEnabled == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_CREATE_DISABLED, datafeedId));
        }
    }

    /**
     * Validates that an update may proceed once the current datafeed config is known. Rejects adding {@code esql_query}
     * to a classic datafeed before rolling-upgrade checks so users see the permanent error immediately.
     */
    public static void validateDatafeedUpdate(DatafeedConfig current, DatafeedUpdate update, ClusterState state) {
        if (current.getEsqlQuery() == null && update.getEsqlQuery() != null) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(Messages.DATAFEED_ESQL_UPDATE_ADD_QUERY_NOT_ALLOWED, current.getId())
            );
        }
        Optional<String> unsupportedReason = unsupportedReason(update.minRequiredTransportVersion(), state);
        if (unsupportedReason.isPresent()) {
            throw unsupportedUpdateException(current.getId(), unsupportedReason.get());
        }
    }

    private static void validateClusterSupportsMinTransportVersion(
        String datafeedId,
        Optional<Tuple<TransportVersion, String>> minRequired,
        ClusterState state
    ) {
        Optional<String> unsupportedReason = unsupportedReason(minRequired, state);
        if (unsupportedReason.isPresent()) {
            throw unsupportedCreateException(datafeedId, unsupportedReason.get());
        }
    }

    private static ElasticsearchStatusException unsupportedCreateException(String datafeedId, String unsupportedReason) {
        return ExceptionsHelper.badRequestException(
            Messages.getMessage(Messages.DATAFEED_ESQL_CREATE_UPGRADE_IN_PROGRESS, datafeedId, unsupportedReason)
        );
    }

    private static ElasticsearchStatusException unsupportedUpdateException(String datafeedId, String unsupportedReason) {
        return ExceptionsHelper.badRequestException(
            Messages.getMessage(Messages.DATAFEED_ESQL_UPDATE_UPGRADE_IN_PROGRESS, datafeedId, unsupportedReason)
        );
    }

    /**
     * On the coordinating node, reject ES|QL-shaped updates before they are forwarded to an older master.
     */
    public static void validateDatafeedUpdateTransportOnCoordinator(DatafeedUpdate update, ClusterState state) {
        Optional<String> unsupportedReason = unsupportedReason(update.minRequiredTransportVersion(), state);
        if (unsupportedReason.isPresent()) {
            throw unsupportedUpdateException(update.getId(), unsupportedReason.get());
        }
    }

    /**
     * Rejects a stored datafeed that needs ES|QL datafeed support the cluster does not have yet (rolling upgrade in
     * progress) or the feature flag does not enable on this node.
     */
    public static void validateEsqlDatafeedEnabled(
        DatafeedConfig datafeedConfig,
        ClusterState state,
        String upgradeInProgressMessage,
        String disabledMessage
    ) {
        if (unsupportedReason(datafeedConfig.minRequiredTransportVersion(), state).isPresent()) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(upgradeInProgressMessage, datafeedConfig.getId()));
        }
        if (datafeedConfig.getEsqlQuery() != null && MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled() == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(disabledMessage, datafeedConfig.getId()));
        }
    }
}
