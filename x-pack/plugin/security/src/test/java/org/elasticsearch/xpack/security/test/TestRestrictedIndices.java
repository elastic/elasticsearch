
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.test;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.security.Security;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TestRestrictedIndices {

    public static final Automaton RESTRICTED_INDICES_AUTOMATON;
    public static final IndexNameExpressionResolver RESOLVER;

    static {
        Security securityPlugin = new Security(Settings.EMPTY, createTempDir());
        SystemIndices systemIndices = new SystemIndices(Map.of(
            securityPlugin.getClass().getSimpleName(),
            new Feature(securityPlugin.getFeatureName(), securityPlugin.getFeatureDescription(),
                securityPlugin.getSystemIndexDescriptors(Settings.EMPTY)),
            "async-search-mock",
            new Feature("async search mock", "fake async search for restricted indices", List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
                    .setDescription("Async search results")
                    .setPrimaryIndex(XPackPlugin.ASYNC_RESULTS_INDEX)
                    .setMappings(mockMappings())
                    .setSettings(Settings.EMPTY)
                    .setVersionMetaKey("version")
                    .setOrigin(ASYNC_SEARCH_ORIGIN)
                    .build()
            ))));
        RESTRICTED_INDICES_AUTOMATON = systemIndices.getSystemNameAutomaton();
        RESOLVER = TestIndexNameExpressionResolver.newInstance(systemIndices);
    }

    private TestRestrictedIndices() {}

    private static XContentBuilder mockMappings() {
        try {
            XContentBuilder builder = jsonBuilder()
                .startObject()
                    .startObject(SINGLE_MAPPING_NAME)
                        .startObject("_meta")
                            .field("version", Version.CURRENT)
                        .endObject()
                        .field("dynamic", "strict")
                        .startObject("properties")
                        .endObject()
                    .endObject()
                .endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
