/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import java.util.Set;

final class EcsOTelResourceAttributes {

    /**
     * The set of ECS (Elastic Common Schema) field names that are mapped to OpenTelemetry resource attributes,
     * as defined by the OpenTelemetry Semantic Conventions.
     * The list is produced by the {@code ResourceAttributesTests#testAttributesSetUpToDate} test.
     *
     * @see <a href="https://github.com/open-telemetry/semantic-conventions">OpenTelemetry Semantic Conventions</a>
     */
    static final Set<String> LATEST = Set.of(
        "agent.type",
        "agent.build.original",
        "agent.name",
        "agent.id",
        "agent.ephemeral_id",
        "agent.version",
        "container.image.tag",
        "device.model.identifier",
        "container.image.hash.all",
        "service.node.name",
        "process.pid",
        "device.id",
        "host.mac",
        "host.type",
        "container.id",
        "cloud.availability_zone",
        "host.ip",
        "container.name",
        "container.image.name",
        "device.model.name",
        "host.name",
        "host.id",
        "process.executable",
        "user_agent.original",
        "service.environment",
        "cloud.region",
        "service.name",
        "faas.name",
        "device.manufacturer",
        "process.args",
        "host.architecture",
        "cloud.provider",
        "container.runtime",
        "service.version",
        "cloud.service.name",
        "cloud.account.id",
        "process.command_line",
        "faas.version"
    );

    private EcsOTelResourceAttributes() {}
}
