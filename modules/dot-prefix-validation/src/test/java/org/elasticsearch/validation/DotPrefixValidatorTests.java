/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

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
    private final OperatorValidator<?> opV = new OperatorValidator<>(true);
    private final NonOperatorValidator<?> nonOpV = new NonOperatorValidator<>(true);

    private static ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        List<String> allowed = new ArrayList<>(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getDefault(Settings.EMPTY));
        // Add a new allowed pattern for testing
        allowed.add("\\.potato\\d+");
        Settings settings = Settings.builder()
            .put(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getKey(), Strings.collectionToCommaDelimitedString(allowed))
            .build();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
    }

    public void testValidation() {

        nonOpV.validateIndices(Set.of("regular"));
        opV.validateIndices(Set.of("regular"));
        assertFails(Set.of(".regular"));
        opV.validateIndices(Set.of(".regular"));
        assertFails(Set.of("first", ".second"));
        assertFails(Set.of("<.regular-{MM-yy-dd}>"));
        assertFails(Set.of(".this_index_contains_an_excepted_pattern.ml-annotations-1"));

        // Test ignored names
        nonOpV.validateIndices(Set.of(".elastic-connectors-v1"));
        nonOpV.validateIndices(Set.of(".elastic-connectors-sync-jobs-v1"));
        nonOpV.validateIndices(Set.of(".ml-state"));
        nonOpV.validateIndices(Set.of(".ml-state-000001"));
        nonOpV.validateIndices(Set.of(".ml-stats-000001"));
        nonOpV.validateIndices(Set.of(".ml-anomalies-unrelated"));

        // Test ignored patterns
        nonOpV.validateIndices(Set.of(".ml-annotations-21309"));
        nonOpV.validateIndices(Set.of(".ml-annotations-2"));
        nonOpV.validateIndices(Set.of(".ml-state-21309"));
        nonOpV.validateIndices(Set.of(">.ml-state-21309>"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2.3"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2.3-2024-01-01"));
        nonOpV.validateIndices(Set.of("<.slo-observability.sli-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2.3"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2.3-2024-01-01"));
        nonOpV.validateIndices(Set.of("<.slo-observability.summary-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        nonOpV.validateIndices(Set.of(".entities.v1.latest.builtin_services_from_ecs_data"));
        nonOpV.validateIndices(Set.of(".entities.v92.latest.eggplant.potato"));
        nonOpV.validateIndices(Set.of("<.entities.v12.latest.eggplant-{M{yyyy-MM-dd|UTC}}>"));

        // Test pattern added to the settings
        nonOpV.validateIndices(Set.of(".potato5"));
        nonOpV.validateIndices(Set.of("<.potato5>"));
    }

    private void assertFails(Set<String> indices) {
        var validator = new NonOperatorValidator<>(false);
        validator.validateIndices(indices);
        assertWarnings(
            "Index ["
                + indices.stream().filter(i -> i.startsWith(".") || i.startsWith("<.")).toList().get(0)
                + "] name begins with a dot (.), which is deprecated, and will not be allowed in a future Elasticsearch version."
        );
    }

    private class NonOperatorValidator<R> extends DotPrefixValidator<R> {

        private final boolean assertNoWarnings;

        private NonOperatorValidator(boolean assertNoWarnings) {
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
        private OperatorValidator(boolean assertNoWarnings) {
            super(assertNoWarnings);
        }

        @Override
        boolean isInternalRequest() {
            return true;
        }
    }
}
