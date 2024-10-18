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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DotPrefixValidatorTests extends ESTestCase {
    private final OperatorValidator<?> opV = new OperatorValidator<>();
    private final NonOperatorValidator<?> nonOpV = new NonOperatorValidator<>();
    private static final Set<Setting<?>> settings;

    private static ClusterService clusterService;
    private static ClusterSettings clusterSettings;

    static {
        Set<Setting<?>> cSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        cSettings.add(DotPrefixValidator.VALIDATE_DOT_PREFIXES);
        settings = cSettings;
    }

    @BeforeClass
    public static void beforeClass() {
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
    }

    public void testValidation() {

        nonOpV.validateIndices(Set.of("regular"));
        opV.validateIndices(Set.of("regular"));
        assertFails(Set.of(".regular"));
        opV.validateIndices(Set.of(".regular"));
        assertFails(Set.of("first", ".second"));
        assertFails(Set.of("<.regular-{MM-yy-dd}>"));

        // Test ignored names
        nonOpV.validateIndices(Set.of(".elastic-connectors-v1"));
        nonOpV.validateIndices(Set.of(".elastic-connectors-sync-jobs-v1"));
        nonOpV.validateIndices(Set.of(".ml-state"));
        nonOpV.validateIndices(Set.of(".ml-anomalies-unrelated"));

        // Test ignored patterns
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
    }

    private void assertFails(Set<String> indices) {
        nonOpV.validateIndices(indices);
        assertWarnings(
            "Index ["
                + indices.stream().filter(i -> i.startsWith(".") || i.startsWith("<.")).toList().getFirst()
                + "] name begins with a dot (.), which is deprecated, and will not be allowed in a future Elasticsearch version."
        );
    }

    private class NonOperatorValidator<R> extends DotPrefixValidator<R> {

        private NonOperatorValidator() {
            super(new ThreadContext(Settings.EMPTY), clusterService);
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
        boolean isInternalRequest() {
            return false;
        }
    }

    private class OperatorValidator<R> extends NonOperatorValidator<R> {
        @Override
        boolean isInternalRequest() {
            return true;
        }
    }
}
