/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DotPrefixValidatorTests extends ESTestCase {
    private static ClusterService statefulClusterService;
    private static ClusterService statelessClusterService;

    private final OperatorValidator<?> statefulOpV = new OperatorValidator<>(statefulClusterService, true);
    private final NonOperatorValidator<?> statefulNonOpV = new NonOperatorValidator<>(statefulClusterService, true);
    private final OperatorValidator<?> statelessOpV = new OperatorValidator<>(statelessClusterService, true);
    private final NonOperatorValidator<?> statelessNonOpV = new NonOperatorValidator<>(statelessClusterService, true);

    @BeforeClass
    public static void beforeClass() {
        List<String> allowed = new ArrayList<>(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getDefault(Settings.EMPTY));
        // Add a new allowed pattern for testing
        allowed.add("\\.potato\\d+");
        Settings statefulSettings = Settings.builder()
            .put(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getKey(), Strings.collectionToCommaDelimitedString(allowed))
            .build();
        statefulClusterService = mock(ClusterService.class);
        ClusterSettings statefulClusterSettings = new ClusterSettings(
            statefulSettings,
            Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING)
        );
        when(statefulClusterService.getClusterSettings()).thenReturn(statefulClusterSettings);
        when(statefulClusterService.getSettings()).thenReturn(statefulSettings);
        when(statefulClusterService.threadPool()).thenReturn(mock(ThreadPool.class));

        Settings statelessSettings = Settings.builder()
            .put(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getKey(), Strings.collectionToCommaDelimitedString(allowed))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        statelessClusterService = mock(ClusterService.class);
        ClusterSettings statelessClusterSettings = new ClusterSettings(
            statelessSettings,
            Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING)
        );
        when(statelessClusterService.getClusterSettings()).thenReturn(statelessClusterSettings);
        when(statelessClusterService.getSettings()).thenReturn(statelessSettings);
        when(statelessClusterService.threadPool()).thenReturn(mock(ThreadPool.class));
    }

    public void testValidation() {

        assertIgnored(Set.of("regular"));
        assertFails(Set.of(".regular"));
        statefulOpV.validateIndices(Set.of(".regular"));
        statelessOpV.validateIndices(Set.of(".regular"));
        assertFails(Set.of("first", ".second"));
        assertFails(Set.of("<.regular-{MM-yy-dd}>"));
        assertFails(Set.of(".this_index_contains_an_excepted_pattern.ml-annotations-1"));

        // Test ignored names
        assertIgnored(Set.of(".elastic-connectors-v1"));
        assertIgnored(Set.of(".elastic-connectors-sync-jobs-v1"));
        assertIgnored(Set.of(".ml-state"));
        assertIgnored(Set.of(".ml-state-000001"));
        assertIgnored(Set.of(".ml-stats-000001"));
        assertIgnored(Set.of(".ml-anomalies-unrelated"));

        // Test ignored patterns
        assertIgnored(Set.of(".ml-annotations-21309"));
        assertIgnored(Set.of(".ml-annotations-2"));
        assertIgnored(Set.of(".ml-state-21309"));
        assertIgnored(Set.of("<.ml-state-21309>"));
        assertIgnored(Set.of(".slo-observability.sli-v2"));
        assertIgnored(Set.of(".slo-observability.sli-v2.3"));
        assertIgnored(Set.of(".slo-observability.sli-v2.3-2024-01-01"));
        assertIgnored(Set.of("<.slo-observability.sli-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        assertIgnored(Set.of(".slo-observability.summary-v2"));
        assertIgnored(Set.of(".slo-observability.summary-v2.3"));
        assertIgnored(Set.of(".slo-observability.summary-v2.3-2024-01-01"));
        assertIgnored(Set.of("<.slo-observability.summary-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        assertIgnored(Set.of(".entities.v1.latest.builtin_services_from_ecs_data"));
        assertIgnored(Set.of(".entities.v1.history.2025-09-16.security_host_default"));
        assertIgnored(Set.of(".entities.v2.history.2025-09-16.security_user_custom"));
        assertIgnored(Set.of(".entities.v5.reset.security_user_custom"));
        assertIgnored(Set.of(".entities.v1.latest.noop"));
        assertIgnored(Set.of(".entities.v92.latest.eggplant.potato"));
        assertIgnored(Set.of("<.entities.v12.latest.eggplant-{M{yyyy-MM-dd|UTC}}>"));
        assertIgnored(Set.of(".monitoring-es-8-thing"));
        assertIgnored(Set.of("<.monitoring-es-8-thing>"));
        assertIgnored(Set.of(".monitoring-logstash-8-thing"));
        assertIgnored(Set.of("<.monitoring-logstash-8-thing>"));
        assertIgnored(Set.of(".monitoring-kibana-8-thing"));
        assertIgnored(Set.of("<.monitoring-kibana-8-thing>"));
        assertIgnored(Set.of(".monitoring-beats-8-thing"));
        assertIgnored(Set.of("<.monitoring-beats-8-thing>"));
        assertIgnored(Set.of(".monitoring-ent-search-8-thing"));
        assertIgnored(Set.of("<.monitoring-ent-search-8-thing>"));

        // Test pattern added to the settings
        assertIgnored(Set.of(".potato5"));
        assertIgnored(Set.of("<.potato5>"));
    }

    private void assertFails(Set<String> indices) {
        /*
         * This method asserts the key difference between stateful and stateless mode -- statful just logs a deprecation warning, while
         * stateful throws an exception.
         */
        var statefulValidator = new NonOperatorValidator<>(statefulClusterService, false);
        statefulValidator.validateIndices(indices);
        assertWarnings(
            "Index ["
                + indices.stream().filter(i -> i.startsWith(".") || i.startsWith("<.")).toList().get(0)
                + "] name begins with a dot (.), which is deprecated, and will not be allowed in a future Elasticsearch version."
        );

        var statelessValidator = new NonOperatorValidator<>(statelessClusterService, false);
        assertThrows(IllegalArgumentException.class, () -> statelessValidator.validateIndices(indices));
    }

    private void assertIgnored(Set<String> indices) {
        statefulNonOpV.validateIndices(indices);
        statefulOpV.validateIndices(indices);
        statelessNonOpV.validateIndices(indices);
        statelessOpV.validateIndices(indices);
    }

    private class NonOperatorValidator<R> extends DotPrefixValidator<R> {

        private final boolean assertNoWarnings;

        private NonOperatorValidator(ClusterService clusterService, boolean assertNoWarnings) {
            super(new ThreadContext(Settings.EMPTY), clusterService);
            this.assertNoWarnings = assertNoWarnings;
        }

        @Override
        protected Set<String> getIndicesFromRequest(Object request) {
            return Set.of();
        }

        @Override
        public String actionName() {
            return "";
        }

        @Override
        void validateIndices(@Nullable Set<String> indices) {
            super.validateIndices(indices);
            if (assertNoWarnings) {
                assertWarnings();
            }
        }

        @Override
        boolean isInternalRequest() {
            return false;
        }
    }

    private class OperatorValidator<R> extends NonOperatorValidator<R> {
        private OperatorValidator(ClusterService clusterService, boolean assertNoWarnings) {
            super(clusterService, assertNoWarnings);
        }

        @Override
        boolean isInternalRequest() {
            return true;
        }
    }
}
