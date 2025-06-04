/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class GetDatabaseConfigurationActionResponseTests extends AbstractWireSerializingTestCase<GetDatabaseConfigurationAction.Response> {
    @Override
    protected Writeable.Reader<GetDatabaseConfigurationAction.Response> instanceReader() {
        return GetDatabaseConfigurationAction.Response::new;
    }

    @Override
    protected GetDatabaseConfigurationAction.Response createTestInstance() {
        return new GetDatabaseConfigurationAction.Response(
            GetDatabaseConfigurationActionNodeResponseTests.getRandomDatabaseConfigurationMetadata(),
            getTestClusterName(),
            getTestNodeResponses(),
            getTestFailedNodeExceptions()
        );
    }

    @Override
    protected GetDatabaseConfigurationAction.Response mutateInstance(GetDatabaseConfigurationAction.Response instance) throws IOException {
        return null;
    }

    private ClusterName getTestClusterName() {
        return new ClusterName(randomAlphaOfLength(30));
    }

    private List<GetDatabaseConfigurationAction.NodeResponse> getTestNodeResponses() {
        return randomList(0, 20, GetDatabaseConfigurationActionNodeResponseTests::getRandomDatabaseConfigurationActionNodeResponse);
    }

    private List<FailedNodeException> getTestFailedNodeExceptions() {
        return randomList(
            0,
            5,
            () -> new FailedNodeException(
                randomAlphaOfLength(10),
                randomAlphaOfLength(20),
                new ElasticsearchException(randomAlphaOfLength(10))
            )
        );
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
