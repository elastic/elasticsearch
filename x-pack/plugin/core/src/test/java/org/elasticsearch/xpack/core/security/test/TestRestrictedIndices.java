
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.test;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_TOKENS_ALIAS;

public class TestRestrictedIndices {

    public static final Automaton RESTRICTED_INDICES_AUTOMATON;
    public static final IndexNameExpressionResolver RESOLVER;

    static {
        SystemIndices systemIndices = new SystemIndices(Map.of(
            "security-mock",
            new Feature("security-mock", "fake security for test restricted indices", List.of(
                SystemIndexDescriptor.builder()
                    // This can't just be `.security-*` because that would overlap with the tokens index pattern
                    .setIndexPattern(".security-[0-9]+")
                    .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7)
                    .setDescription("Contains Security configuration")
                    .setMappings(mockMappings())
                    .setSettings(Settings.EMPTY)
                    .setAliasName(SECURITY_MAIN_ALIAS)
                    .setIndexFormat(7)
                    .setVersionMetaKey("version")
                    .setOrigin(SECURITY_ORIGIN)
                    .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
                    .build(),
                SystemIndexDescriptor.builder()
                    .setIndexPattern(".security-tokens-[0-9]+")
                    .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7)
                    .setDescription("Contains auth token data")
                    .setMappings(mockMappings())
                    .setSettings(Settings.EMPTY)
                    .setAliasName(SECURITY_TOKENS_ALIAS)
                    .setIndexFormat(7)
                    .setVersionMetaKey("version")
                    .setOrigin(SECURITY_ORIGIN)
                    .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
                    .build()
            )),
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
