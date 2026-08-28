/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class FullTextFunctionIT extends AbstractEsqlIntegTestCase {

    private final String matchingClause;
    private final EsqlCapabilities.Cap requiredCapability;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), KqlPlugin.class);
    }

    @ParametersFactory
    public static List<Object[]> params() {
        return List.of(
            new Object[] { "match_phrase(keyword_field, \"fox search\")", EsqlCapabilities.Cap.MATCH_PHRASE_FUNCTION },
            new Object[] { "match(keyword_field, \"fox search\")", EsqlCapabilities.Cap.MATCH_FUNCTION },
            new Object[] { "keyword_field:\"fox search\"", EsqlCapabilities.Cap.MATCH_FUNCTION },
            new Object[] { "qstr(\"keyword_field:\\\"fox search\\\"\")", EsqlCapabilities.Cap.QSTR_FUNCTION },
            new Object[] { "kql(\"keyword_field: \\\"fox search\\\"\")", EsqlCapabilities.Cap.KQL_FUNCTION }
        );
    }

    public FullTextFunctionIT(String matchingClause, EsqlCapabilities.Cap requiredCapability) {
        this.matchingClause = matchingClause;
        this.requiredCapability = requiredCapability;
    }

    @Before
    public void setupIndicesAcrossDataNodes() {
        assumeTrue("Requires unmapped_fields=\"load\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        assumeTrue("Requires " + requiredCapability.name(), requiredCapability.isEnabled());

        internalCluster().ensureAtLeastNumDataNodes(2);
        String keywordNode = randomDataNode().getName();
        String otherNode = randomValueOtherThan(keywordNode, () -> randomDataNode().getName());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_kw")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", keywordNode)
                )
                .setMapping("""
                    { "properties": { "keyword_field": { "type": "keyword" } } }""")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx_other")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", otherNode)
                )
                .setMapping("""
                    { "properties": { "other": { "type": "keyword" } } }""")
        );

        indexDoc("idx_kw", "1", "keyword_field", "fox search");
        indexDoc("idx_other", "1", "other", "x");
        refresh("idx_kw", "idx_other");
    }

    public void testFullTextFunctionOrUnmappedFieldIsNullWithLoadAcrossRemoteDataNodes() {
        var query = String.format(Locale.ROOT, """
            SET unmapped_fields="load";
            FROM idx_kw, idx_other METADATA _index
            | WHERE %s OR unmapped_field_bar IS NULL
            | KEEP _index, keyword_field, other
            | SORT _index
            """, matchingClause);

        try (var resp = run(query)) {
            assertThat(resp.isPartial(), equalTo(false));
            assertColumnNames(resp.columns(), List.of("_index", "keyword_field", "other"));
            assertColumnTypes(resp.columns(), List.of("keyword", "keyword", "keyword"));

            var values = getValuesList(resp);
            assertThat(values.size(), equalTo(2));
            assertThat(values.get(0), contains("idx_kw", "fox search", null));
            assertThat(values.get(1), contains("idx_other", null, "x"));
        }
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
