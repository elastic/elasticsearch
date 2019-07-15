package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.logging.ESLogMessage;

import java.util.Map;

public class ClusterChangeLogMessage extends ESLogMessage {

    public ClusterChangeLogMessage(ClusterHealthStatus previousHealth, ClusterHealthStatus currentHealth, String reason) {
        super(fieldMap(previousHealth, currentHealth, reason), "Cluster health status changed.");
    }

    private static Map<String, Object> fieldMap(ClusterHealthStatus previousHealth, ClusterHealthStatus currentHealth, String reason) {

        return Map.of("previous.health", previousHealth, "current.health", currentHealth, "reason", reason);
    }
}

