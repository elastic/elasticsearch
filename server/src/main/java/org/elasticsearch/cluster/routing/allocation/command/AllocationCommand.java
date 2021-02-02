/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Optional;

/**
 * A command to move shards in some way.
 *
 * Commands are registered in {@link NetworkModule}.
 */
public interface AllocationCommand extends NamedWriteable, ToXContentObject {

    /**
     * Get the name of the command
     * @return name of the command
     */
    String name();

    /**
     * Executes the command on a {@link RoutingAllocation} setup
     * @param allocation {@link RoutingAllocation} to modify
     * @throws org.elasticsearch.ElasticsearchException if something happens during reconfiguration
     */
    RerouteExplanation execute(RoutingAllocation allocation, boolean explain);

    @Override
    default String getWriteableName() {
        return name();
    }

    /**
     * Returns any feedback the command wants to provide for logging. This message should be appropriate to expose to the user after the
     * command has been applied
     */
    default Optional<String> getMessage() {
        return Optional.empty();
    }
}
