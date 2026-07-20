/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * See <a href="https://github.com/elastic/elasticsearch/issues/152322">#152322</a>.
 * With {@code unmapped_fields="load"}, a multi-field ({@code languages} + {@code languages.long}) that is mapped in one queried
 * index and absent in another becomes a partially-unmapped ("two-legged PUNK") field. Its sub-field then resolves to a
 * coordinator-only conflict field ({@code CompactInvalidMappedField}) that lives inside the parent's {@code properties} map.
 * {@code ResolveTwoLeggedPunksInEsRelation} rewrites the parent via {@code typeSpecificConvert}; if that rewrite copies the
 * parent's {@code properties} it drags the un-transportable sub-field onto the wire, and serializing the data-node plan fragment
 * throws {@code "CompactInvalidMappedField shouldn't be transported"}. The fix builds the converted field with empty
 * {@code properties}.
 */
public class LoadUnmappedMultiFieldTransportIT extends AbstractEsqlIntegTestCase {

    public void testLoadPartiallyUnmappedMultiFieldSerializesToRemoteDataNode() {
        assumeTrue("Requires unmapped_fields=\"load\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        internalCluster().ensureAtLeastNumDataNodes(2);
        String mappedNode = randomDataNode().getName();
        String unmappedNode = randomValueOtherThan(mappedNode, () -> randomDataNode().getName());

        // idx_mapped maps `languages` as an integer with a `long` multi-field; both become two-legged PUNKs once unioned with idx_unmapped.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_mapped")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", mappedNode)
                )
                .setMapping("""
                    { "properties": { "languages": { "type": "integer", "fields": { "long": { "type": "long" } } } } }""")
        );
        // idx_unmapped does not map `languages` at all, so under load it is the unmapped leg of the PUNK.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_unmapped")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", unmappedNode)
                )
                .setMapping("""
                    { "properties": { "message": { "type": "keyword" } } }""")
        );

        indexDoc("idx_mapped", "1", "languages", 2);
        indexDoc("idx_unmapped", "1", "message", "hello");
        refresh("idx_mapped", "idx_unmapped");

        try (
            var resp = run(
                "SET unmapped_fields=\"load\"; FROM idx_mapped, idx_unmapped | KEEP languages, languages.long | SORT languages NULLS LAST"
            )
        ) {
            assertThat(resp.isPartial(), equalTo(false));
            assertColumnNames(resp.columns(), List.of("languages", "languages.long"));
            assertColumnTypes(resp.columns(), List.of("integer", "long"));

            var values = getValuesList(resp);
            assertThat(values.size(), equalTo(2));
            assertThat(values.get(0), contains(2, 2L));
            assertThat(values.get(1), contains(null, null));
        }
    }

    /**
     * See <a href="https://github.com/elastic/elasticsearch/issues/154011">#154011</a>.
     * A PUNK {@code long} field (mapped in one index, absent in another) queried without
     * an explicit {@code ::long} cast, with each index pinned to a different data node.
     * The {@link org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveTwoLeggedPunksInEsRelation}
     * rule rewrites the field to a {@code CompactMultiTypeEsField} whose unmapped leg loads
     * from {@code _source} as BYTES_REF and converts to LONG via a {@code TypeConverter}.
     * That converter must survive plan serialisation to remote data nodes; if it is dropped,
     * {@code ValuesSourceReaderOperator.sanityCheckBlock} throws
     * {@code element_type [BYTES_REF] NOT IN (NULL, LONG)}.
     */
    public void testPunkLongStatsImplicitAcrossNodes() {
        assumeTrue(
            "Requires two-legged PUNK auto-cast",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_UNMAPPED_LOAD_AUTO_CAST_TWO_LEGGED_PUNKS.isEnabled()
        );

        internalCluster().ensureAtLeastNumDataNodes(2);
        String mappedNode = randomDataNode().getName();
        String unmappedNode = randomValueOtherThan(mappedNode, () -> randomDataNode().getName());

        // event_duration mapped as long on one node ...
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_mapped_duration")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", mappedNode)
                )
                .setMapping("""
                    { "properties": { "event_duration": { "type": "long" } } }""")
        );
        // ... and unmapped (but present in _source) on the other: the unmapped leg of the PUNK.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_unmapped_duration")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", unmappedNode)
                )
                .setMapping("""
                    { "properties": { "message": { "type": "keyword" } } }""")
        );

        long expectedSum = 0;
        for (int i = 0; i < 10; i++) {
            long val = (i + 1) * 1000L;
            indexDoc("idx_mapped_duration", Integer.toString(i), "event_duration", val);
            expectedSum += val;
        }
        for (int i = 10; i < 20; i++) {
            long val = (i + 1) * 1000L;
            // Stored in _source without a mapping; the PUNK loads it via DefaultShardContextForUnmappedField.
            indexDoc("idx_unmapped_duration", Integer.toString(i), "event_duration", val);
            expectedSum += val;
        }
        refresh("idx_mapped_duration", "idx_unmapped_duration");

        // Implicit access — no ::long cast — exercises ResolveTwoLeggedPunksInEsRelation.
        try (
            var resp = run(
                "SET unmapped_fields=\"load\"; FROM idx_mapped_duration, idx_unmapped_duration"
                    + " | STATS s = SUM(event_duration), c = COUNT(event_duration)"
            )
        ) {
            assertThat(resp.isPartial(), equalTo(false));
            assertColumnNames(resp.columns(), List.of("s", "c"));
            assertColumnTypes(resp.columns(), List.of("long", "long"));

            var values = getValuesList(resp);
            assertThat(values.size(), equalTo(1));
            assertThat(values.get(0).get(0), equalTo(expectedSum));
            assertThat(values.get(0).get(1), equalTo(20L));
        }
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
