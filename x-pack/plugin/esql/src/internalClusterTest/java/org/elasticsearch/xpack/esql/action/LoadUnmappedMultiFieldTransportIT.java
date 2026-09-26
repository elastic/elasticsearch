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

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
