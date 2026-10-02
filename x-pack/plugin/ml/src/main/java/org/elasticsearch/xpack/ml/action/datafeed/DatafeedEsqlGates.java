/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
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
     * The checks that creating a datafeed must pass before it is persisted, shared by the put datafeed action and the put
     * anomaly detection job action (which can embed a datafeed): the cluster must support the config (no rolling upgrade
     * in progress for a config that needs a newer transport version) and ES|QL datafeeds must be enabled when the config
     * has an {@code esql_query}.
     *
     * @param datafeedId    id of the datafeed being created, used in the message
     * @param minRequired   the minimum transport version (and reason) the datafeed config requires, if any
     * @param usesEsqlQuery whether the datafeed config has an {@code esql_query}
     * @param esqlDatafeedsEnabled whether ES|QL datafeeds are enabled on this node
     * @return the rejection to fail the request with, or empty if the datafeed may be created
     */
    public static Optional<Exception> createRejection(
        String datafeedId,
        Optional<Tuple<TransportVersion, String>> minRequired,
        boolean usesEsqlQuery,
        ClusterState state,
        boolean esqlDatafeedsEnabled
    ) {
        Optional<String> unsupportedReason = unsupportedReason(minRequired, state);
        if (unsupportedReason.isPresent()) {
            return Optional.of(unsupportedCreateException(datafeedId, unsupportedReason.get()));
        }
        if (usesEsqlQuery && esqlDatafeedsEnabled == false) {
            return Optional.of(
                ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_CREATE_DISABLED, datafeedId))
            );
        }
        return Optional.empty();
    }

    public static Exception unsupportedCreateException(String datafeedId, String unsupportedReason) {
        return ExceptionsHelper.badRequestException(
            "Cannot create datafeed [{}] while a cluster upgrade is in progress ({}); "
                + "wait for the cluster to finish upgrading and try again.",
            datafeedId,
            unsupportedReason
        );
    }

    /**
     * Rejects a stored datafeed that needs ES|QL datafeed support the cluster does not have yet (rolling upgrade in
     * progress) or the feature flag does not enable on this node.
     *
     * @param upgradeInProgressMessageKey {@link Messages} key used when the cluster has not finished upgrading
     * @param disabledMessageKey          {@link Messages} key used when the feature flag is off
     */
    static void validateEsqlDatafeedEnabled(
        DatafeedConfig datafeedConfig,
        ClusterState state,
        String upgradeInProgressMessageKey,
        String disabledMessageKey
    ) {
        if (unsupportedReason(datafeedConfig.minRequiredTransportVersion(), state).isPresent()) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(upgradeInProgressMessageKey, datafeedConfig.getId()));
        }
        if (datafeedConfig.getEsqlQuery() != null && MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled() == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(disabledMessageKey, datafeedConfig.getId()));
        }
    }
}
