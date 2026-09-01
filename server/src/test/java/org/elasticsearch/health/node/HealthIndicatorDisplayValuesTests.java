/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.are;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getNodeName;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getSortedUniqueValuesString;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedProjectIndices;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.indices;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.indicesComparatorByPriorityAndProjectIndex;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.regularNoun;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.regularVerb;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.these;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthIndicatorDisplayValuesTests extends ESTestCase {

    public void testGetNodeName() {
        String nodeId = randomAlphaOfLength(10);
        String nodeName = randomAlphaOfLength(8);
        assertThat(getNodeName(DiscoveryNodeUtils.create(nodeName, nodeId)), equalTo("[" + nodeId + "][" + nodeName + "]"));

        // DiscoveryNode stores a null construction-time name as "", so exercise the null branch with a stub.
        DiscoveryNode nodeWithNullName = mock(DiscoveryNode.class);
        when(nodeWithNullName.getId()).thenReturn(nodeId);
        when(nodeWithNullName.getName()).thenReturn(null);
        assertThat(getNodeName(nodeWithNullName), equalTo("[" + nodeId + "]"));
    }

    public void testGetTruncatedProjectIndices() {
        ProjectId projectId = ProjectId.DEFAULT;
        Metadata metadata = metadata(
            projectId,
            indexMetadata("high", 10),
            indexMetadata("mid-b", 5),
            indexMetadata("mid-a", 5),
            indexMetadata("low", 1)
        );

        assertThat(getTruncatedProjectIndices(Set.of(), metadata, false), equalTo(""));

        // Sorted by descending priority, then by display name; indices absent from metadata sort last (priority -1).
        Set<ProjectIndexName> indices = Set.of(
            projectIndex(projectId, "low"),
            projectIndex(projectId, "mid-b"),
            projectIndex(projectId, "missing"),
            projectIndex(projectId, "high"),
            projectIndex(projectId, "mid-a")
        );
        assertThat(getTruncatedProjectIndices(indices, metadata, false), equalTo("high, mid-a, mid-b, low, missing"));
        assertThat(
            getTruncatedProjectIndices(indices, metadata, true),
            equalTo("default/high, default/mid-a, default/mid-b, default/low, default/missing")
        );

        // More than 10 indices are truncated with a trailing ellipsis; highest-priority indices are kept.
        Set<ProjectIndexName> manyIndices = IntStream.rangeClosed(1, 12)
            .mapToObj(i -> projectIndex(projectId, "index-" + String.format(Locale.ROOT, "%02d", i)))
            .collect(Collectors.toSet());
        Metadata manyMetadata = metadata(
            projectId,
            IntStream.rangeClosed(1, 12)
                .mapToObj(i -> indexMetadata("index-" + String.format(Locale.ROOT, "%02d", i), i))
                .toArray(IndexMetadata[]::new)
        );
        assertThat(
            getTruncatedProjectIndices(manyIndices, manyMetadata, false),
            equalTo("index-12, index-11, index-10, index-09, index-08, index-07, index-06, index-05, index-04, index-03, ...")
        );
    }

    public void testGetTruncatedProjectIndicesAcrossProjects() {
        ProjectId projectA = ProjectId.fromId("project-a");
        ProjectId projectB = ProjectId.fromId("project-b");
        Metadata metadata = Metadata.builder()
            .put(ProjectMetadata.builder(projectA).put(indexMetadata("shared", 5), true).put(indexMetadata("only-a", 1), true))
            .put(ProjectMetadata.builder(projectB).put(indexMetadata("shared", 10), true).put(indexMetadata("only-b", 1), true))
            .build();

        Set<ProjectIndexName> indices = Set.of(
            projectIndex(projectA, "shared"),
            projectIndex(projectB, "shared"),
            projectIndex(projectA, "only-a"),
            projectIndex(projectB, "only-b")
        );

        // With multi-project display, secondary sort uses "projectId/indexName".
        assertThat(
            getTruncatedProjectIndices(indices, metadata, true),
            equalTo("project-b/shared, project-a/shared, project-a/only-a, project-b/only-b")
        );
        // Without project ids in the display string, equal-priority names sort alphabetically by index name only.
        assertThat(getTruncatedProjectIndices(indices, metadata, false), equalTo("shared, shared, only-a, only-b"));
    }

    public void testGetSortedUniqueValuesString() {
        List<String> values = List.of("charlie", "alpha", "bravo", "alpha", "delta");

        assertThat(getSortedUniqueValuesString(List.of(), String::toString), equalTo(""));
        assertThat(getSortedUniqueValuesString(values, String::toString), equalTo("alpha, bravo, charlie, delta"));
        assertThat(
            getSortedUniqueValuesString(values, v -> v.startsWith("a") || v.startsWith("d"), String::toString),
            equalTo("alpha, delta")
        );
        assertThat(getSortedUniqueValuesString(values, v -> false, String::toString), equalTo(""));
        assertThat(getSortedUniqueValuesString(List.of(3, 1, 2, 1), i -> "value-" + i), equalTo("value-1, value-2, value-3"));
    }

    public void testIndices() {
        assertThat(indices(1), equalTo("index"));
        assertThat(indices(0), equalTo("indices"));
        assertThat(indices(2), equalTo("indices"));
        assertThat(indices(randomIntBetween(2, 100)), equalTo("indices"));
    }

    public void testAre() {
        assertThat(are(1), equalTo("is"));
        assertThat(are(0), equalTo("are"));
        assertThat(are(2), equalTo("are"));
        assertThat(are(randomIntBetween(2, 100)), equalTo("are"));
    }

    public void testThese() {
        assertThat(these(1), equalTo("this"));
        assertThat(these(0), equalTo("these"));
        assertThat(these(2), equalTo("these"));
        assertThat(these(randomIntBetween(2, 100)), equalTo("these"));
    }

    public void testRegularNoun() {
        String noun = randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT);
        assertThat(regularNoun(noun, 1), equalTo(noun));
        assertThat(regularNoun(noun, 0), equalTo(noun + "s"));
        assertThat(regularNoun(noun, 2), equalTo(noun + "s"));
        assertThat(regularNoun(noun, randomIntBetween(2, 100)), equalTo(noun + "s"));
    }

    public void testRegularVerb() {
        String verb = randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT);
        assertThat(regularVerb(verb, 1), equalTo(verb + "s"));
        assertThat(regularVerb(verb, 0), equalTo(verb));
        assertThat(regularVerb(verb, 2), equalTo(verb));
        assertThat(regularVerb(verb, randomIntBetween(2, 100)), equalTo(verb));
    }

    public void testIndicesComparatorByPriorityAndProjectIndex() {
        ProjectId projectId = ProjectId.DEFAULT;
        Metadata metadata = metadata(
            projectId,
            indexMetadata("high", 10),
            indexMetadata("same-b", 5),
            indexMetadata("same-a", 5),
            indexMetadata("low", 1)
        );

        Comparator<ProjectIndexName> comparator = indicesComparatorByPriorityAndProjectIndex(metadata, false);
        List<ProjectIndexName> sorted = List.of(
            projectIndex(projectId, "low"),
            projectIndex(projectId, "same-b"),
            projectIndex(projectId, "missing"),
            projectIndex(projectId, "high"),
            projectIndex(projectId, "same-a")
        ).stream().sorted(comparator).toList();

        assertThat(
            sorted,
            equalTo(
                List.of(
                    projectIndex(projectId, "high"),
                    projectIndex(projectId, "same-a"),
                    projectIndex(projectId, "same-b"),
                    projectIndex(projectId, "low"),
                    projectIndex(projectId, "missing")
                )
            )
        );

        ProjectId projectA = ProjectId.fromId("aaa");
        ProjectId projectZ = ProjectId.fromId("zzz");
        Metadata multiProjectMetadata = Metadata.builder()
            .put(ProjectMetadata.builder(projectA).put(indexMetadata("index", 1), true))
            .put(ProjectMetadata.builder(projectZ).put(indexMetadata("index", 1), true))
            .build();
        Comparator<ProjectIndexName> multiProjectComparator = indicesComparatorByPriorityAndProjectIndex(multiProjectMetadata, true);
        assertThat(multiProjectComparator.compare(projectIndex(projectA, "index"), projectIndex(projectZ, "index")), lessThan(0));
        assertThat(multiProjectComparator.compare(projectIndex(projectZ, "index"), projectIndex(projectA, "index")), greaterThan(0));
    }

    private static ProjectIndexName projectIndex(ProjectId projectId, String indexName) {
        return new ProjectIndexName(projectId, indexName);
    }

    private static IndexMetadata indexMetadata(String indexName, int priority) {
        return IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_PRIORITY, priority))
            .build();
    }

    private static Metadata metadata(ProjectId projectId, IndexMetadata... indices) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
        for (IndexMetadata indexMetadata : indices) {
            project.put(indexMetadata, true);
        }
        return Metadata.builder().put(project).build();
    }
}
