
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.test;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;

public class TestRestrictedIndices {

    public static final RestrictedIndices RESTRICTED_INDICES;
    public static final IndexNameExpressionResolver RESOLVER;

    public static final String SECURITY_MAIN_ALIAS = ".security";
    public static final String INTERNAL_SECURITY_MAIN_INDEX_7 = ".security-7";
    public static final String INTERNAL_SECURITY_MAIN_INDEX_6 = ".security-6";

    public static final String SECURITY_TOKENS_ALIAS = ".security-tokens";
    public static final String INTERNAL_SECURITY_TOKENS_INDEX_7 = ".security-tokens-7";

    public static final Set<String> SAMPLE_RESTRICTED_NAMES = Collections.unmodifiableSet(
        Sets.newHashSet(
            SECURITY_MAIN_ALIAS,
            INTERNAL_SECURITY_MAIN_INDEX_6,
            INTERNAL_SECURITY_MAIN_INDEX_7,
            INTERNAL_SECURITY_TOKENS_INDEX_7,
            SECURITY_TOKENS_ALIAS
        )
    );

    static {
        List<Feature> features = new ArrayList<>();
        features.add(
            new Feature(
                "security-mock",
                "fake security for test restricted indices",
                List.of(getMainSecurityDescriptor(), getSecurityTokensDescriptor())
            )
        );
        features.add(new Feature("async search mock", "fake async search for restricted indices", List.of(getAsyncSearchDescriptor())));
        features.add(
            new Feature(
                "kibana-mock",
                "fake kibana for testing restricted indices",
                List.of(
                    getKibanaSavedObjectsDescriptor(),
                    getReportingIndexDescriptor(),
                    getApmAgentConfigDescriptor(),
                    getApmCustomLinkDescriptor()
                )
            )
        );

        // From here, we have very minimal mock features that only supply system index patterns,
        // not settings or mock mappings.
        features.add(
            new Feature(
                "enrich-mock",
                "fake enrich for restricted indices tests",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(".enrich-*", "enrich pattern"))
            )
        );
        features.add(
            new Feature(
                "fleet-mock",
                "fake fleet for restricted indices tests",
                List.of(
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-actions~(-results*)", "fleet actions"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-agents*", "fleet agents"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-enrollment-api-keys*", "fleet enrollment"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-policies-[0-9]+*", "fleet policies"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-policies-leader*", "fleet policies leader"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-servers*", "fleet servers"),
                    SystemIndexDescriptorUtils.createUnmanaged(".fleet-artifacts*", "fleet artifacts")
                ),
                List.of(
                    new SystemDataStreamDescriptor(
                        ".fleet-actions-results",
                        "fleet actions results",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        new ComposableIndexTemplate(
                            List.of(".fleet-actions-results"),
                            null,
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        ),
                        Map.of(),
                        List.of("fleet", "kibana"),
                        null
                    )
                )
            )
        );
        features.add(
            new Feature(
                "ingest-geoip-mock",
                "fake geoip for restricted indices tests",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(".geoip_databases*", "geoip databases"))
            )
        );
        features.add(
            new Feature(
                "logstash-mock",
                "fake logstash for restricted indices tests",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(".logstash*", "logstash"))
            )
        );
        features.add(
            new Feature(
                "machine-learning-mock",
                "fake machine learning for restricted indices tests",
                List.of(
                    SystemIndexDescriptorUtils.createUnmanaged(".ml-meta*", "machine learning meta"),
                    SystemIndexDescriptorUtils.createUnmanaged(".ml-config*", "machine learning config"),
                    SystemIndexDescriptorUtils.createUnmanaged(".ml-inference*", "machine learning inference")
                )
            )
        );
        features.add(
            new Feature(
                "searchable-snapshots-mock",
                "fake searchable snapshots for restricted indices tests",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(".snapshot-blob-cache*", "snapshot blob cache"))
            )
        );
        features.add(
            new Feature(
                "transform-mock",
                "fake transform for restricted indices tests",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(".transform-internal-*", "transform internal"))
            )
        );
        features.add(
            new Feature(
                "watcher-mock",
                "fake watcher for restricted indices tests",
                List.of(
                    SystemIndexDescriptorUtils.createUnmanaged(".watches*", "watches"),
                    SystemIndexDescriptorUtils.createUnmanaged(".triggered-watches*", "triggered watches")
                )
            )
        );

        SystemIndices systemIndices = new SystemIndices(features);
        RESTRICTED_INDICES = new RestrictedIndices(systemIndices.getSystemNameAutomaton());
        RESOLVER = TestIndexNameExpressionResolver.newInstance(systemIndices);
    }

    private static SystemIndexDescriptor.Builder getInitializedDescriptorBuilder(int indexFormat) {
        return SystemIndexDescriptor.builder()
            .setMappings(mockMappings())
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), indexFormat).build())
            .setVersionMetaKey("version");
    }

    private static SystemIndexDescriptor getMainSecurityDescriptor() {
        return getInitializedDescriptorBuilder(7)
            // This can't just be `.security-*` because that would overlap with the tokens index pattern
            .setIndexPattern(".security-[0-9]+*")
            .setPrimaryIndex(INTERNAL_SECURITY_MAIN_INDEX_7)
            .setDescription("Contains Security configuration")
            .setAliasName(SECURITY_MAIN_ALIAS)
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(7, "526016472"))
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static SystemIndexDescriptor getSecurityTokensDescriptor() {
        return getInitializedDescriptorBuilder(7).setIndexPattern(".security-tokens-[0-9]+*")
            .setPrimaryIndex(INTERNAL_SECURITY_TOKENS_INDEX_7)
            .setDescription("Contains auth token data")
            .setAliasName(SECURITY_TOKENS_ALIAS)
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(7, "526016472"))
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static SystemIndexDescriptor getAsyncSearchDescriptor() {
        return getInitializedDescriptorBuilder(0).setIndexPattern(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
            .setDescription("Async search results")
            .setPrimaryIndex(XPackPlugin.ASYNC_RESULTS_INDEX)
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(0, "526016471"))
            .setOrigin(ASYNC_SEARCH_ORIGIN)
            .build();
    }

    private static SystemIndexDescriptor getKibanaSavedObjectsDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".kibana_*")
            .setDescription("Kibana saved objects system index")
            .setAliasName(".kibana")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
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
            .setIndexPattern(".apm-agent-configuration*")
            .setDescription("system index for APM agent configuration")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
            .build();
    }

    private static SystemIndexDescriptor getApmCustomLinkDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".apm-custom-link*")
            .setDescription("system index for APM custom links")
            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of("kibana"))
            .build();
    }

    private TestRestrictedIndices() {}

    private static XContentBuilder mockMappings() {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
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
