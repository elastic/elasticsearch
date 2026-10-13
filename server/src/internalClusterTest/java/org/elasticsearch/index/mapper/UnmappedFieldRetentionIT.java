/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.resizeRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests that an index records, in {@link IndexSettings#RETAINS_UNMAPPED_FIELDS_SETTING}, that one of its mappings can keep a
 * field it does not declare, and that the record follows the documents wherever they are copied.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class UnmappedFieldRetentionIT extends AbstractSnapshotIntegTestCase {

    private static final String SETTING = IndexSettings.RETAINS_UNMAPPED_FIELDS_SETTING.getKey();

    public void testRecordedWhenTheIndexIsCreated() {
        assertAcked(prepareCreate("tight").setMapping("field", "type=keyword"));
        assertRetains("tight", false);

        assertAcked(prepareCreate("loose").setMapping("""
            { "dynamic": false, "properties": { "field": { "type": "keyword" } } }"""));
        assertRetains("loose", true);

        assertAcked(prepareCreate("empty"));
        assertRetains("empty", false);
    }

    public void testRecordedFromTheTemplateOfTheIndex() throws Exception {
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("templated-*"))
            .template(new Template(indexSettings(1, 0).build(), new CompressedXContent("""
                { "properties": { "object": { "type": "object", "enabled": false } } }"""), null))
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("retention-template").indexTemplate(template)
            )
        );
        prepareIndex("templated-1").setSource("field", "value").get();
        assertRetains("templated-1", true);
    }

    /**
     * Documents indexed while a mapping kept unmapped fields still hold them after the mapping is tightened.
     */
    public void testRecordedWhenTheMappingIsUpdatedAndKeptFromThenOn() {
        assertAcked(prepareCreate("index").setMapping("field", "type=keyword"));
        assertRetains("index", false);

        // A dynamic mapping update does not make the mapping keep unmapped fields.
        prepareIndex("index").setSource("dynamically_mapped", "value").get();
        assertRetains("index", false);

        assertAcked(indicesAdmin().preparePutMapping("index").setSource(Map.of("dynamic", false)));
        assertRetains("index", true);

        assertAcked(indicesAdmin().preparePutMapping("index").setSource(Map.of("dynamic", "strict")));
        assertRetains("index", true);

        assertAcked(indicesAdmin().prepareClose("index"));
        assertAcked(indicesAdmin().prepareOpen("index"));
        assertRetains("index", true);
    }

    public void testCannotBeSetByAUser() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            prepareCreate("index").setSettings(Settings.builder().put(SETTING, true))
        );
        assertThat(e.getMessage(), containsString("private index setting [" + SETTING + "] can not be set explicitly"));

        assertAcked(prepareCreate("index").setMapping("""
            { "dynamic": false }"""));
        e = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareUpdateSettings("index").setSettings(Settings.builder().put(SETTING, false))
        );
        assertThat(e.getMessage(), containsString(SETTING));
        assertRetains("index", true);
    }

    public void testNotRemovedByAWildcard() {
        assertAcked(prepareCreate("index").setMapping("""
            { "dynamic": false }"""));
        assertAcked(indicesAdmin().preparePutMapping("index").setSource(Map.of("dynamic", true)));
        assertAcked(indicesAdmin().prepareUpdateSettings("index").setSettings(Settings.builder().putNull("index.mapping.*")));
        assertRetains("index", true);

        assertAcked(indicesAdmin().prepareClose("index"));
        for (String pattern : new String[] { "index.mapping.*", "index.mapping.retains*" }) {
            assertAcked(indicesAdmin().prepareUpdateSettings("index").setSettings(Settings.builder().putNull(pattern)));
        }
        assertAcked(indicesAdmin().prepareOpen("index"));
        assertRetains("index", true);
    }

    /**
     * A runtime field can be removed, which leaves the values documents hold for it without a field.
     */
    public void testRecordedForRuntimeFields() {
        assertAcked(prepareCreate("declared").setMapping("""
            { "runtime": { "field": { "type": "keyword" } } }"""));
        assertRetains("declared", true);
        prepareIndex("declared").setSource("field", "value").get();
        assertAcked(indicesAdmin().preparePutMapping("declared").setSource(Map.of("runtime", Collections.singletonMap("field", null))));
        assertRetains("declared", true);

        assertAcked(prepareCreate("dynamic").setMapping("""
            { "dynamic": "runtime" }"""));
        assertRetains("dynamic", true);

        assertAcked(prepareCreate("below").setMapping("""
            { "properties": { "object": { "type": "object", "dynamic": "runtime" } } }"""));
        assertRetains("below", true);
    }

    /**
     * A copy holds the documents of its source, so it starts with what the source recorded, whatever its own mapping says.
     */
    public void testCopiedToAnIndexMadeFromAnother() {
        assertAcked(prepareCreate("source").setSettings(indexSettings(2, 0).put("index.number_of_routing_shards", 4)).setMapping("""
            { "dynamic": false }"""));
        prepareIndex("source").setSource("unmapped", "value").get();
        assertAcked(indicesAdmin().preparePutMapping("source").setSource(Map.of("dynamic", "strict")));
        assertFalse(mapping("source").contains("\"dynamic\":\"false\""));
        assertRetains("source", true);

        assertAcked(prepareCreate("tight-source").setSettings(indexSettings(1, 0)).setMapping("field", "type=keyword"));

        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source", "tight-source");
        ensureGreen("source", "tight-source");
        for (boolean copySettings : new boolean[] { false, true }) {
            String suffix = copySettings ? "-copy" : "";
            resize(ResizeType.CLONE, "source", "clone" + suffix, 2, copySettings);
            assertRetains("clone" + suffix, true);
            resize(ResizeType.SPLIT, "source", "split" + suffix, 4, copySettings);
            assertRetains("split" + suffix, true);
            resize(ResizeType.SHRINK, "source", "shrink" + suffix, 1, copySettings);
            assertRetains("shrink" + suffix, true);
        }

        resize(ResizeType.CLONE, "tight-source", "tight-clone", 1, false);
        assertRetains("tight-clone", false);
    }

    public void testRestoredWithTheIndex() {
        createRepository("repository", "fs");
        assertAcked(prepareCreate("index").setMapping("""
            { "dynamic": false }"""));
        prepareIndex("index").setSource("unmapped", "value").get();
        assertAcked(indicesAdmin().preparePutMapping("index").setSource(Map.of("dynamic", true)));
        assertRetains("index", true);
        createSnapshot("repository", "snapshot", List.of("index"));

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snapshot")
            .setRenamePattern("index")
            .setRenameReplacement("restored")
            .setWaitForCompletion(true)
            .get();
        assertRetains("restored", true);

        Exception e = expectThrows(
            SnapshotRestoreException.class,
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snapshot")
                .setRenamePattern("index")
                .setRenameReplacement("without")
                .setIgnoreIndexSettings(SETTING)
        );
        assertThat(e.getMessage(), containsString("cannot remove setting [" + SETTING + "] on restore"));

        e = expectThrows(
            SnapshotRestoreException.class,
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snapshot")
                .setRenamePattern("index")
                .setRenameReplacement("modified")
                .setIndexSettings(Settings.builder().put(SETTING, false))
        );
        assertThat(e.getMessage(), containsString("cannot modify setting [" + SETTING + "] on restore"));
    }

    private void resize(ResizeType type, String source, String target, int shards, boolean copySettings) {
        ResizeRequest request = resizeRequest(type, source, target, indexSettings(shards, 0).putNull("index.blocks.write"));
        request.setCopySettings(copySettings ? Boolean.TRUE : null);
        assertAcked(client().execute(TransportResizeAction.TYPE, request));
        ensureGreen(target);
    }

    private String mapping(String index) {
        return indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get().mappings().get(index).source().string();
    }

    /** The settings of the index as its shards see them. */
    private IndexSettings shardSettings(String index) {
        ensureGreen(index);
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService != null) {
                return indexService.getIndexSettings();
            }
        }
        throw new AssertionError("no shard of [" + index + "]");
    }

    private void assertRetains(String index, boolean expected) {
        IndexMetadata metadata = clusterService().state().metadata().getProject().index(index);
        assertThat(index, IndexSettings.RETAINS_UNMAPPED_FIELDS_SETTING.get(metadata.getSettings()), equalTo(expected));
        assertThat(index, metadata.getSettings().hasValue(SETTING), equalTo(expected));
        if (metadata.getState() == IndexMetadata.State.OPEN) {
            assertThat(index, shardSettings(index).retainsUnmappedFields(), equalTo(expected));
        }
        // Neither the mapping nor the settings an API returns carry it, so they can be copied to another index as they are.
        assertThat(index, mapping(index), not(containsString("retains_unmapped_fields")));
        Settings returned = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, index).get().getIndexToSettings().get(index);
        assertFalse(index, returned.hasValue(SETTING));
        var getIndex = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(index).get();
        assertFalse(index, getIndex.getSettings().get(index).hasValue(SETTING));
    }
}
