
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
import java.util.HashMap;
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
        Map<String, Feature> featureMap = new HashMap<>();
        featureMap.put("security-mock",
            new Feature("security-mock", "fake security for test restricted indices", List.of(
                getMainSecurityDescriptor(),
                getSecurityTokensDescriptor())));
        featureMap.put("async-search-mock",
            new Feature("async search mock", "fake async search for restricted indices", List.of(
                getAsyncSearchDescriptor())));
        featureMap.put("kibana-mock",
            new Feature("kibana-mock", "fake kibana for testing restricted indices", List.of(
                getKibanaSavedObjectsDescriptor(),
                getReportingIndexDescriptor(),
                getApmAgentConfigDescriptor(),
                getApmCustomLinkDescriptor())));

        // From here, we have very minimal mock features that only supply system index patterns,
        // not settings or mock mappings.
        featureMap.put("enrich-mock",
            new Feature("enrich-mock", "fake enrich for restricted indices tests", List.of(
                new SystemIndexDescriptor(".enrich-*", "enrich pattern"))));
        featureMap.put("fleet-mock",
            new Feature("fleet-mock", "fake fleet for restricted indices tests", List.of(
                new SystemIndexDescriptor(".fleet-actions~(-results*)", "fleet actions"),
                new SystemIndexDescriptor(".fleet-agents*", "fleet agents"),
                new SystemIndexDescriptor(".fleet-enrollment-api-keys*", "fleet enrollment"),
                new SystemIndexDescriptor(".fleet-policies-[0-9]+", "fleet policies"),
                new SystemIndexDescriptor(".fleet-policies-leader*", "fleet policies leader"),
                new SystemIndexDescriptor(".fleet-servers*", "fleet servers"),
                new SystemIndexDescriptor(".fleet-artifacts*", "fleet artifacts"))));
        featureMap.put("ingest-geoip-mock",
            new Feature("ingest-geoip-mock", "fake geoip for restricted indices tests", List.of(
                new SystemIndexDescriptor(".geoip_databases", "geoip databases"))));
        featureMap.put("logstash-mock",
            new Feature("logstash-mock", "fake logstash for restricted indices tests", List.of(
                new SystemIndexDescriptor(".logstash", "logstash"))));
        featureMap.put("machine-learning-mock",
            new Feature("machine-learning-mock", "fake machine learning for restricted indices tests", List.of(
                new SystemIndexDescriptor(".ml-meta*", "machine learning meta"),
                new SystemIndexDescriptor(".ml-config*", "machine learning config"),
                new SystemIndexDescriptor(".ml-inference*", "machine learning inference"))));
        featureMap.put("searchable-snapshots-mock",
            new Feature("searchable-snapshots-mock", "fake searchable snapshots for restricted indices tests", List.of(
                new SystemIndexDescriptor(".snapshot-blob-cache", "snapshot blob cache"))));
        featureMap.put("transform-mock",
            new Feature("transform-mock", "fake transform for restricted indices tests", List.of(
                new SystemIndexDescriptor(".transform-internal-*", "transform internal"))));
        featureMap.put("watcher-mock",
            new Feature("watcher-mock", "fake watcher for restricted indices tests", List.of(
                new SystemIndexDescriptor(".watches*", "watches"),
                new SystemIndexDescriptor(".triggered-watches*", "triggered watches"))));

        SystemIndices systemIndices = new SystemIndices(featureMap);
        RESTRICTED_INDICES_AUTOMATON = systemIndices.getSystemNameAutomaton();
        RESOLVER = TestIndexNameExpressionResolver.newInstance(systemIndices);
    }

    private static SystemIndexDescriptor.Builder getInitializedDescriptorBuilder() {
        return SystemIndexDescriptor.builder()
            .setMappings(mockMappings())
            .setSettings(Settings.EMPTY)
            .setVersionMetaKey("version");
    }

    private static SystemIndexDescriptor getMainSecurityDescriptor() {
        return getInitializedDescriptorBuilder()
            // This can't just be `.security-*` because that would overlap with the tokens index pattern
            .setIndexPattern(".security-[0-9]+")
            .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7)
            .setDescription("Contains Security configuration")
            .setAliasName(SECURITY_MAIN_ALIAS)
            .setIndexFormat(7)
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static SystemIndexDescriptor getSecurityTokensDescriptor() {
        return getInitializedDescriptorBuilder()
            .setIndexPattern(".security-tokens-[0-9]+")
            .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7)
            .setDescription("Contains auth token data")
            .setAliasName(SECURITY_TOKENS_ALIAS)
            .setIndexFormat(7)
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static SystemIndexDescriptor getAsyncSearchDescriptor() {
        return getInitializedDescriptorBuilder()
            .setIndexPattern(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
            .setDescription("Async search results")
            .setPrimaryIndex(XPackPlugin.ASYNC_RESULTS_INDEX)
            .setOrigin(ASYNC_SEARCH_ORIGIN)
            .build();
    }

    private static SystemIndexDescriptor getKibanaSavedObjectsDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".kibana_*")
            .setDescription("Kibana saved objects system index")
            .setAliasName(".kibana")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins( List.of("kibana"))
            .build();
    }

    private static SystemIndexDescriptor getReportingIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".reporting-*")
            .setDescription("system index for reporting")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
            .build();
    }

    private static SystemIndexDescriptor getApmAgentConfigDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".apm-agent-configuration")
            .setDescription("system index for APM agent configuration")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
            .build();
    }

    private static SystemIndexDescriptor getApmCustomLinkDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".apm-custom-link")
            .setDescription("system index for APM custom links")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
            .build();
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
