/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorFiltering;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;

import java.io.IOException;
import java.util.List;

public class UpdateConnectorFilteringActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorFilteringAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorFilteringAction.Request> instanceReader() {
        return UpdateConnectorFilteringAction.Request::new;
    }

    @Override
    protected UpdateConnectorFilteringAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorFilteringAction.Request(
            connectorId,
            randomList(10, ConnectorTestUtils::getRandomConnectorFiltering),
            ConnectorTestUtils.getRandomConnectorFiltering().getActive().getAdvancedSnippet(),
            ConnectorTestUtils.getRandomConnectorFiltering().getActive().getRules()
        );
    }

    @Override
    protected UpdateConnectorFilteringAction.Request mutateInstance(UpdateConnectorFilteringAction.Request instance) throws IOException {
        String originalConnectorId = instance.getConnectorId();
        List<ConnectorFiltering> filtering = instance.getFiltering();
        FilteringAdvancedSnippet advancedSnippet = instance.getAdvancedSnippet();
        List<FilteringRule> rules = instance.getRules();
        switch (between(0, 3)) {
            case 0 -> originalConnectorId = randomValueOtherThan(originalConnectorId, () -> randomUUID());
            case 1 -> filtering = randomValueOtherThan(filtering, () -> randomList(10, ConnectorTestUtils::getRandomConnectorFiltering));
            case 2 -> advancedSnippet = randomValueOtherThan(
                advancedSnippet,
                () -> ConnectorTestUtils.getRandomConnectorFiltering().getActive().getAdvancedSnippet()
            );
            case 3 -> rules = randomValueOtherThan(rules, () -> ConnectorTestUtils.getRandomConnectorFiltering().getActive().getRules());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UpdateConnectorFilteringAction.Request(originalConnectorId, filtering, advancedSnippet, rules);
    }

    @Override
    protected UpdateConnectorFilteringAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorFilteringAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorFilteringAction.Request mutateInstanceForVersion(
        UpdateConnectorFilteringAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
