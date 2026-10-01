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
final class DatafeedEsqlGates {

    private DatafeedEsqlGates() {}

    /**
     * @param minRequired the minimum transport version (and reason) a datafeed config or update requires, if any
     * @return the reason the cluster cannot support the config or update yet, or empty if the minimum transport
     * version is satisfied or not required
     */
    static Optional<String> unsupportedReason(Optional<Tuple<TransportVersion, String>> minRequired, ClusterState state) {
        if (minRequired.isPresent() && state.getMinTransportVersion().supports(minRequired.get().v1()) == false) {
            return Optional.of(minRequired.get().v2());
        }
        return Optional.empty();
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
