/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration test for _terms_enum behavior when Cross-Project Search (CPS) is enabled
 * ({@code serverless.cross_project.enabled=true}).
 */
public class TermsEnumCpsIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, CpsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    /**
     * Regression test for the bug introduced by #152302:
     * with CPS enabled, a _terms_enum request against a non-existent concrete index returned 200 with an empty
     * terms list instead of 404.
     *
     * Root cause: {@code AsyncBroadcastAction.start()} short-circuited directly to
     * {@code listener.onResponse} when {@code expectedOps == 0} (no local shards, no remote fan-out),
     * bypassing {@code finishHim()} where the {@link
     * org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator} check runs.
     *
     * The fix is to call {@code finishHim(true)} in the zero-ops branch so that the validator
     * sees the {@code CONCRETE_RESOURCE_NOT_VISIBLE} result recorded by the security layer and
     * returns an {@link IndexNotFoundException}.
     *
     * Because this test runs without the security plugin (to keep it simple), the
     * {@link ResolvedIndexExpressions} that the security layer would normally populate is
     * pre-populated manually to exercise the same code path in
     * {@code TransportTermsEnumAction.AsyncBroadcastAction.finishHim()}.
     */
    public void testMissingConcreteIndexReturnsWith404WhenCpsEnabled() {
        // The security layer (IndicesAndAliasesResolver) records CONCRETE_RESOURCE_NOT_VISIBLE for
        // a concrete index that does not exist when CPS-lenient options (ignoreUnavailable=true) are used.
        // Pre-populate that result here to exercise the finishHim() validation path.
        ResolvedIndexExpressions.Builder expressionsBuilder = ResolvedIndexExpressions.builder();
        expressionsBuilder.addExpressions(
            "non-existent-index",
            new HashSet<>(),
            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
            Set.of()
        );
        ResolvedIndexExpressions resolvedExpressions = expressionsBuilder.build();

        TermsEnumRequest request = new TermsEnumRequest("non-existent-index").field("some.keyword");
        // Replicate what RestTermsEnumAction does for CPS-enabled clusters: opt the request into
        // cross-project resolution so that crossProjectModeDecider.resolvesCrossProject() returns true.
        request.indicesOptions(
            IndicesOptions.builder(request.indicesOptions())
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .build()
        );
        request.setResolvedIndexExpressions(resolvedExpressions);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> client().execute(TermsEnumAction.INSTANCE, request).get());
        assertThat(ExceptionsHelper.unwrapCause(ex.getCause()), instanceOf(IndexNotFoundException.class));
    }

    /**
     * Control test: a concrete index that is missing but {@code ignore_unavailable=true} is set should return an empty terms list,
     * not a 404. The {@link org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator} respects
     * {@code ignore_unavailable} and does not treat {@code CONCRETE_RESOURCE_NOT_VISIBLE} as a not-found failure
     * when that flag is set.
     */
    public void testMissingConcreteIndexReturnsEmptyWhenIgnoreUnavailableAndCpsEnabled() {
        ResolvedIndexExpressions.Builder expressionsBuilder = ResolvedIndexExpressions.builder();
        expressionsBuilder.addExpressions(
            "non-existent-index",
            new HashSet<>(),
            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
            Set.of()
        );
        ResolvedIndexExpressions resolvedExpressions = expressionsBuilder.build();

        TermsEnumRequest request = new TermsEnumRequest("non-existent-index").field("some.keyword");
        request.indicesOptions(
            IndicesOptions.builder(request.indicesOptions())
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                .build()
        );
        request.setResolvedIndexExpressions(resolvedExpressions);

        TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, request).actionGet();
        assertThat(response.getTerms(), empty());
        assertTrue(response.isComplete());
    }

    /**
     * Control test: a wildcard expression that matches no indices should return an empty terms list when CPS is enabled.
     * {@code allow_no_indices=true} is the default for {@link TermsEnumRequest}, so zero wildcard matches is not an error.
     * This confirms that routing the zero-ops case through {@code finishHim()} does not change the behaviour for wildcards.
     */
    public void testWildcardMatchingNoIndicesReturnsEmptyWhenCpsEnabled() {
        TermsEnumRequest request = new TermsEnumRequest("non-existent-*").field("some.keyword");
        request.indicesOptions(
            IndicesOptions.builder(request.indicesOptions())
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .build()
        );

        TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, request).actionGet();
        assertThat(response.getTerms(), empty());
        assertTrue(response.isComplete());
    }

    /**
     * Registers {@code serverless.cross_project.enabled} as a valid node-scoped setting.
     * In production the setting is registered by a serverless-only module; in tests this
     * lightweight plugin stands in so that the node does not reject it as unknown.
     */
    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }
}
