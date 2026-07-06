/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;

/**
 * Round-trips {@link ExecuteAbstractionRequest} — the execution-half request that ships an abstraction's NAME plus the
 * coordinator's expected output schema (the B1 schema-drift guard). The expected-schema field is a {@code List<Attribute>}
 * that must serialize through a {@link org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput}; this test exercises that
 * path and the {@link org.elasticsearch.action.IndicesRequest} surface ({@code indices()} == the abstraction name).
 */
public class ExecuteAbstractionRequestTests extends AbstractWireSerializingTestCase<ExecuteAbstractionRequest> {

    @Override
    protected Writeable.Reader<ExecuteAbstractionRequest> instanceReader() {
        return in -> new ExecuteAbstractionRequest(in, new SerializationTestUtils.TestNameIdMapper());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        return new NamedWriteableRegistry(writeables);
    }

    @Override
    protected ExecuteAbstractionRequest createTestInstance() {
        String query = "FROM " + randomAlphaOfLength(8);
        ExecuteAbstractionRequest request = new ExecuteAbstractionRequest(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomConfiguration(query, randomTables()),
            randomAlphaOfLength(8),
            AbstractPhysicalPlanSerializationTests.randomFieldAttributes(1, 5, false)
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected ExecuteAbstractionRequest mutateInstance(ExecuteAbstractionRequest in) throws IOException {
        String clusterAlias = in.clusterAlias();
        String sessionId = in.sessionId();
        var configuration = in.configuration();
        String abstractionName = in.abstractionName();
        List<Attribute> expected = in.expectedAttributes();
        switch (between(0, 4)) {
            case 0 -> clusterAlias = randomValueOtherThan(clusterAlias, () -> randomAlphaOfLength(10));
            case 1 -> sessionId = randomValueOtherThan(sessionId, () -> randomAlphaOfLength(10));
            case 2 -> configuration = randomValueOtherThan(configuration, ConfigurationTestUtils::randomConfiguration);
            case 3 -> abstractionName = randomValueOtherThan(abstractionName, () -> randomAlphaOfLength(8));
            case 4 -> expected = randomValueOtherThan(
                expected,
                () -> AbstractPhysicalPlanSerializationTests.randomFieldAttributes(1, 5, false)
            );
            default -> throw new AssertionError("invalid value");
        }
        ExecuteAbstractionRequest request = new ExecuteAbstractionRequest(
            clusterAlias,
            sessionId,
            configuration,
            abstractionName,
            expected
        );
        request.setParentTask(in.getParentTask());
        return request;
    }
}
