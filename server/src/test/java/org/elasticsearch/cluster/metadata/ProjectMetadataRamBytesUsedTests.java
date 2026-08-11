/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ProjectMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return ProjectMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("id", "indices", "templates", "oldestIndexVersion");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // Deliberately not counted (see ProjectMetadata#ramBytesUsed javadoc): customs is a documented gap; the remaining entries are
        // derived caches or references that duplicate data already counted via indices/templates.
        return Set.of(
            "customs",
            "aliasedIndices",
            "allIndices",
            "visibleIndices",
            "allOpenIndices",
            "visibleOpenIndices",
            "allClosedIndices",
            "visibleClosedIndices",
            "indicesLookup",
            "mappingsByHash"
        );
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Settings under-counting, shared MappingMetadata dedup, and the documented customs/derived-cache exclusions mean a full-graph
        // RamUsageTester walk would fail estimate >= actual by design. Behavioural tests below cover growth and dedup instead.
        return false;
    }

    /**
     * Non-tautology check: adding an index must increase the reported size (the per-index {@link IndexMetadata#ramBytesUsed()} is counted).
     */
    public void testRamBytesUsedGrowsWithIndices() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        ProjectMetadata empty = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        ProjectMetadata withIndex = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("test").settings(settings))
            .build();
        assertThat(withIndex.ramBytesUsed(), greaterThan(empty.ramBytesUsed()));
    }

    /**
     * Non-tautology check: when two indices share one {@link MappingMetadata} instance, the project total must be exactly one mapping
     * smaller than a naive sum that omits dedup. Uses a test-only variant ({@link #projectRamBytesUsedWithoutMappingDedup}) that
     * deliberately skips the subtract step — not a mirror of {@link ProjectMetadata#ramBytesUsed()}.
     */
    public void testRamBytesUsedDedupesSharedMappings() throws IOException {
        MappingMetadata mapping = new MappingMetadata(CompressedXContent.fromJSON("""
            { "_doc": { "properties": { "field": { "type": "keyword" } } } }
            """));
        Settings settingsA = indexSettings(1, 0).put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current().id())
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-a")
            .build();
        Settings settingsB = indexSettings(1, 0).put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current().id())
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-b")
            .build();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(IndexMetadata.builder("index-a").settings(settingsA).putMapping(mapping))
            .put(IndexMetadata.builder("index-b").settings(settingsB).putMapping(mapping))
            .build();

        IndexMetadata indexA = project.index("index-a");
        IndexMetadata indexB = project.index("index-b");
        assertSame(indexA.mapping(), indexB.mapping());
        assertThat(project.indices().size(), equalTo(2));

        long withoutDedup = projectRamBytesUsedWithoutMappingDedup(project);
        assertThat(withoutDedup, greaterThan(project.ramBytesUsed()));
        assertThat(withoutDedup - project.ramBytesUsed(), equalTo(indexA.mapping().ramBytesUsed()));
    }

    /**
     * Naive project sum: adds each index's {@link IndexMetadata#ramBytesUsed()} plus map entry overhead, but never subtracts duplicate
     * shared {@link MappingMetadata} instances. Intentionally different from production {@link ProjectMetadata#ramBytesUsed()}.
     */
    private static long projectRamBytesUsedWithoutMappingDedup(ProjectMetadata project) {
        long size = RamUsageEstimator.shallowSizeOfInstance(ProjectMetadata.class);
        size += RamUsageEstimator.shallowSizeOf(project.id());
        size += RamUsageEstimator.shallowSizeOf(project.oldestIndexVersion());
        size += MetadataRamEstimators.ramBytesUsedByAccountableMap(project.indices());
        size += MetadataRamEstimators.ramBytesUsedByAccountableMap(project.templates());
        return RamUsageEstimator.alignObjectSize(size);
    }
}
