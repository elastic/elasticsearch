/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Verifies the columnar-mode default behaviour for {@code text} field norms.
 *
 * <p>Columnar mode defaults {@code norms} to {@code false} on {@code text} fields
 * ({@code TextFieldMapper.java:338-345}), unlike standard mode where norms default to
 * {@code true}. This test pins that product decision so it is not accidentally changed, and
 * confirms that an explicit {@code "norms": true} mapping is still honoured.
 */
public class ColumnarTextNormsIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * A bare {@code text} field (no explicit {@code norms}) must have norms disabled in a
     * columnar index.
     */
    public void testDefaultNormsDisabledInColumnarMode() throws Exception {
        assertAcked(
            prepareCreate("idx_default_norms").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("body", "type=text")
        );

        GetMappingsResponse resp = indicesAdmin().prepareGetMappings("idx_default_norms").get();
        @SuppressWarnings("unchecked")
        Map<String, Object> bodyMapping = (Map<String, Object>) ((Map<?, ?>) resp.getMappings()
            .get("idx_default_norms")
            .getSourceAsMap()
            .get("properties")).get("body");

        // columnar mode should have forced norms off
        assertEquals("text field norms must default to false in columnar mode (TextFieldMapper.java:341)", false, bodyMapping.get("norms"));
    }

    /**
     * An explicit {@code "norms": true} on a {@code text} field must be honoured even in
     * columnar mode.
     */
    public void testExplicitNormsEnabledHonouredInColumnarMode() throws Exception {
        assertAcked(
            prepareCreate("idx_explicit_norms").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("{\"properties\":{\"body\":{\"type\":\"text\",\"norms\":true}}}")
        );

        GetMappingsResponse resp = indicesAdmin().prepareGetMappings("idx_explicit_norms").get();
        @SuppressWarnings("unchecked")
        Map<String, Object> bodyMapping = (Map<String, Object>) ((Map<?, ?>) resp.getMappings()
            .get("idx_explicit_norms")
            .getSourceAsMap()
            .get("properties")).get("body");

        assertEquals("explicit norms:true must be preserved in columnar mode", true, bodyMapping.get("norms"));
    }
}
