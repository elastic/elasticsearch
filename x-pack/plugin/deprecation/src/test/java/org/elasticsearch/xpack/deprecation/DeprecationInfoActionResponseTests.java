/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeprecationInfoActionResponseTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Response> {

    @Override
    protected DeprecationInfoAction.Response createTestInstance() {
        List<DeprecationIssue> clusterIssues = randomDeprecationIssues();
        List<DeprecationIssue> nodeIssues = randomDeprecationIssues();
        Map<String, List<DeprecationIssue>> indexIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> dataStreamIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> templateIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> ilmPolicyIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> pluginIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        return new DeprecationInfoAction.Response(
            clusterIssues,
            nodeIssues,
            Map.of(
                "data_streams",
                dataStreamIssues,
                "index_settings",
                indexIssues,
                "templates",
                templateIssues,
                "ilm_policies",
                ilmPolicyIssues
            ),
            pluginIssues
        );
    }

    @Override
    protected DeprecationInfoAction.Response mutateInstance(DeprecationInfoAction.Response instance) {
        List<DeprecationIssue> clusterIssues = instance.getClusterSettingsIssues();
        List<DeprecationIssue> nodeIssues = instance.getNodeSettingsIssues();
        Map<String, List<DeprecationIssue>> indexIssues = instance.getIndexSettingsIssues();
        Map<String, List<DeprecationIssue>> dataStreamIssues = instance.getDataStreamDeprecationIssues();
        Map<String, List<DeprecationIssue>> templateIssues = instance.getTemplateDeprecationIssues();
        Map<String, List<DeprecationIssue>> ilmPolicyIssues = instance.getIlmPolicyDeprecationIssues();
        Map<String, List<DeprecationIssue>> pluginIssues = instance.getPluginSettingsIssues();
        switch (randomIntBetween(1, 7)) {
            case 1 -> clusterIssues = randomValueOtherThan(clusterIssues, DeprecationInfoActionResponseTests::randomDeprecationIssues);
            case 2 -> nodeIssues = randomValueOtherThan(nodeIssues, DeprecationInfoActionResponseTests::randomDeprecationIssues);
            case 3 -> indexIssues = randomValueOtherThan(
                indexIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 4 -> dataStreamIssues = randomValueOtherThan(
                dataStreamIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 5 -> templateIssues = randomValueOtherThan(
                templateIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 6 -> ilmPolicyIssues = randomValueOtherThan(
                ilmPolicyIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 7 -> pluginIssues = randomValueOtherThan(
                pluginIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
        }
        return new DeprecationInfoAction.Response(
            clusterIssues,
            nodeIssues,
            Map.of(
                "data_streams",
                dataStreamIssues,
                "index_settings",
                indexIssues,
                "templates",
                templateIssues,
                "ilm_policies",
                ilmPolicyIssues
            ),
            pluginIssues
        );
    }

    @Override
    protected Writeable.Reader<DeprecationInfoAction.Response> instanceReader() {
        return DeprecationInfoAction.Response::new;
    }

    static DeprecationIssue createTestDeprecationIssue() {
        return createTestDeprecationIssue(randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4))));
    }

    static DeprecationIssue createTestDeprecationIssue(Map<String, Object> metaMap) {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            metaMap
        );
    }

    static DeprecationIssue createTestDeprecationIssue(DeprecationIssue seedIssue, Map<String, Object> metaMap) {
        return new DeprecationIssue(
            seedIssue.getLevel(),
            seedIssue.getMessage(),
            seedIssue.getUrl(),
            seedIssue.getDetails(),
            seedIssue.isResolveDuringRollingUpgrade(),
            metaMap
        );
    }

    static List<DeprecationIssue> randomDeprecationIssues() {
        return Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList());
    }
}
