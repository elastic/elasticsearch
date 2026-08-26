/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptySet;

public class GetDatabaseConfigurationActionNodeResponseTests extends AbstractWireSerializingTestCase<
    GetDatabaseConfigurationAction.NodeResponse> {
    @Override
    protected Writeable.Reader<GetDatabaseConfigurationAction.NodeResponse> instanceReader() {
        return GetDatabaseConfigurationAction.NodeResponse::new;
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse createTestInstance() {
        return getRandomDatabaseConfigurationActionNodeResponse();
    }

    static GetDatabaseConfigurationAction.NodeResponse getRandomDatabaseConfigurationActionNodeResponse() {
        return new GetDatabaseConfigurationAction.NodeResponse(randomDiscoveryNode(), getRandomDatabaseConfigurationMetadata());
    }

    private static DiscoveryNode randomDiscoveryNode() {
        return DiscoveryNodeUtils.builder(randomAlphaOfLength(6)).roles(emptySet()).build();
    }

    static List<DatabaseConfigurationMetadata> getRandomDatabaseConfigurationMetadata() {
        return randomList(
            0,
            20,
            () -> new DatabaseConfigurationMetadata(
                new DatabaseConfiguration(
                    randomAlphaOfLength(20),
                    randomAlphaOfLength(20),
                    randomFrom(
                        List.of(
                            new DatabaseConfiguration.Local(randomAlphaOfLength(10)),
                            new DatabaseConfiguration.Web(),
                            new DatabaseConfiguration.Ipinfo(),
                            new DatabaseConfiguration.Maxmind(randomAlphaOfLength(10))
                        )
                    )
                ),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
        );
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse mutateInstance(GetDatabaseConfigurationAction.NodeResponse instance)
        throws IOException {
        return null;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    DatabaseConfiguration.Provider.class,
                    DatabaseConfiguration.Maxmind.NAME,
                    DatabaseConfiguration.Maxmind::new
                ),
                new NamedWriteableRegistry.Entry(
                    DatabaseConfiguration.Provider.class,
                    DatabaseConfiguration.Ipinfo.NAME,
                    DatabaseConfiguration.Ipinfo::new
                ),
                new NamedWriteableRegistry.Entry(
                    DatabaseConfiguration.Provider.class,
                    DatabaseConfiguration.Local.NAME,
                    DatabaseConfiguration.Local::new
                ),
                new NamedWriteableRegistry.Entry(
                    DatabaseConfiguration.Provider.class,
                    DatabaseConfiguration.Web.NAME,
                    DatabaseConfiguration.Web::new
                )
            )
        );
    }
}
