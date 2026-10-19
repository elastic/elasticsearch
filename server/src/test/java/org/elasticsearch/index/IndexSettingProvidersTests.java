/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.ESTestCase.expectThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IndexSettingProvidersTests extends ESTestCase {

    public void testProvidedSettingsAreDefaults() {
        var additionalSettings = collect(
            List.of(provider(Settings.builder().put("index.a", "provided").put("index.b", "provided").build(), false))
        );
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of()));

        Settings effective = applyTo(additionalSettings, Settings.builder().put("index.a", "configured"));
        assertThat(effective.get("index.a"), equalTo("configured"));
        assertThat(effective.get("index.b"), equalTo("provided"));
    }

    public void testOverrulingSettingsWin() {
        var additionalSettings = collect(
            List.of(
                provider(Settings.builder().put("index.a", "overruled").putNull("overruling_null").build(), true),
                provider(Settings.builder().put("index.b", "provided").build(), false)
            )
        );
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of("index.a", "overruling_null")));

        Settings.Builder requestSettingsBuilder = Settings.builder()
            .put("index.a", "configured")
            .put("index.b", "configured")
            .put("index.c", "configured")
            .put("overruling_null", "configured");
        Settings.Builder effectiveSettingsBuilder = Settings.builder();

        additionalSettings.applyTo(requestSettingsBuilder.build(), effectiveSettingsBuilder, requestSettingsBuilder);
        Settings effective = effectiveSettingsBuilder.build();
        assertThat(effective.get("index.a"), equalTo("overruled"));
        assertThat(effective.get("overruling_null"), nullValue());
        assertThat(effective.get("index.b"), equalTo("configured"));
        assertThat(effective.get("index.c"), equalTo("configured"));

        Set<String> requestSettingNames = requestSettingsBuilder.keys();
        assertThat(requestSettingNames.contains("overruling_null"), equalTo(false));
        assertThat(requestSettingNames.contains("index.a"), equalTo(false));
        assertThat(requestSettingNames.contains("index.b"), equalTo(true));
        assertThat(requestSettingNames.contains("index.c"), equalTo(true));
    }

    public void testNoProviders() {
        var additionalSettings = collect(List.of());
        assertThat(additionalSettings.settings(), equalTo(Settings.EMPTY));
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of()));

        Settings effective = applyTo(additionalSettings, Settings.builder().put("index.a", "configured").putNull("index.b"));
        assertThat(effective, equalTo(Settings.builder().put("index.a", "configured").build()));
    }

    public void testSettingsOfMultipleProvidersAreCombined() {
        var additionalSettings = collect(
            List.of(
                provider(Settings.builder().put("index.a", "a").build(), false),
                provider(Settings.builder().put("index.b", "b").build(), true),
                provider(Settings.builder().put("index.c", "c").build(), false)
            )
        );
        assertThat(
            additionalSettings.settings(),
            equalTo(Settings.builder().put("index.a", "a").put("index.b", "b").put("index.c", "c").build())
        );
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of("index.b")));
    }

    public void testProvidersReceiveTheGivenContext() throws IOException {
        String indexName = randomAlphaOfLength(8);
        String dataStreamName = randomAlphaOfLength(8);
        IndexMode indexMode = randomFrom(IndexMode.values());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        Instant resolvedAt = Instant.ofEpochMilli(randomNonNegativeLong() / 1000);
        Settings templateAndRequest = Settings.builder().put("index.template", "value").build();
        List<CompressedXContent> mappings = List.of(new CompressedXContent("{\"_doc\":{}}"));
        IndexVersion version = IndexVersionUtils.randomVersion();

        var invoked = new AtomicInteger();
        IndexSettingProvider provider = new IndexSettingProvider() {
            @Override
            public void provideAdditionalSettings(
                String index,
                String dataStream,
                IndexMode mode,
                ProjectMetadata project,
                Instant resolved,
                Settings settings,
                List<CompressedXContent> combinedMappings,
                IndexVersion indexVersion,
                Settings.Builder additional
            ) {
                invoked.incrementAndGet();
                assertThat(index, equalTo(indexName));
                assertThat(dataStream, equalTo(dataStreamName));
                assertThat(mode, equalTo(indexMode));
                assertThat(project, sameInstance(projectMetadata));
                assertThat(resolved, equalTo(resolvedAt));
                assertThat(settings, equalTo(templateAndRequest));
                assertThat(combinedMappings, equalTo(mappings));
                assertThat(indexVersion, equalTo(version));
            }

            @Override
            public Set<String> affectedSettings() {
                return Set.of();
            }
        };
        IndexSettingProviders.collectAdditionalSettings(
            List.of(provider),
            indexName,
            dataStreamName,
            indexMode,
            projectMetadata,
            resolvedAt,
            templateAndRequest,
            mappings,
            version
        );
        assertThat(invoked.get(), equalTo(1));
    }

    public void testExplicitNullCancelsProvidedSetting() {
        var additionalSettings = collect(
            List.of(provider(Settings.builder().put("index.a", "provided").put("index.b", "provided").build(), false))
        );
        Settings effective = applyTo(additionalSettings, Settings.builder().putNull("index.a"));
        assertThat(effective.keySet().contains("index.a"), equalTo(false));
        assertThat(effective.get("index.b"), equalTo("provided"));
    }

    public void testOverrulingProviderWinsOverExplicitNull() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "overruled").build(), true)));
        Settings effective = applyTo(additionalSettings, Settings.builder().putNull("index.a"));
        assertThat(effective.get("index.a"), equalTo("overruled"));
    }

    public void testOverrulingProviderWithoutConfiguredValue() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "overruled").build(), true)));
        Settings effective = applyTo(additionalSettings, Settings.builder().put("index.other", "configured"));
        assertThat(effective.get("index.a"), equalTo("overruled"));
        assertThat(effective.get("index.other"), equalTo("configured"));
    }

    public void testApplyToAddsToTheEffectiveBuilderWithoutClearingIt() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings.Builder effective = Settings.builder().put("index.existing", "existing");
        additionalSettings.applyTo(Settings.builder().put("index.b", "configured").build(), effective, Settings.builder());
        assertThat(
            effective.build(),
            equalTo(Settings.builder().put("index.existing", "existing").put("index.a", "provided").put("index.b", "configured").build())
        );
    }

    public void testApplyToWithoutRequestBuilderRemovesNulls() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings.Builder effective = Settings.builder();
        additionalSettings.applyTo(Settings.builder().putNull("index.a").putNull("index.b").build(), effective, null);
        assertThat(effective.build(), equalTo(Settings.EMPTY));
    }

    public void testApplyToKeepsNullsOfTheRequest() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings.Builder effective = Settings.builder();
        Settings.Builder request = Settings.builder().putNull("index.a").putNull("index.b");
        additionalSettings.applyTo(Settings.builder().putNull("index.a").putNull("index.b").build(), effective, request);
        Settings result = effective.build();
        // the nulls cancel the provided value and are preserved so that they also override any value that is applied later
        assertThat(result.keySet(), equalTo(Set.of("index.a", "index.b")));
        assertThat(result.get("index.a"), nullValue());
        assertThat(result.get("index.b"), nullValue());
        assertThat(request.build().keySet(), equalTo(Set.of("index.a", "index.b")));
    }

    public void testApplyToDropsNullsThatAreNotInTheRequest() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings.Builder effective = Settings.builder();
        // the nulls come from the template, the request does not mention the settings
        additionalSettings.applyTo(
            Settings.builder().putNull("index.a").putNull("index.b").build(),
            effective,
            Settings.builder().put("index.c", "requested")
        );
        assertThat(effective.build(), equalTo(Settings.EMPTY));
    }

    /**
     * A provided setting is cancelled by a {@code null} in the request even if the template has a value for it. Pins that the request
     * {@code null} wins over the template value, so the setting resolves to its default.
     */
    public void testApplyToRequestNullWinsOverTemplateValueForProvidedSetting() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings template = Settings.builder().put("index.a", "template").build();
        Settings request = Settings.builder().putNull("index.a").build();
        Settings.Builder requestBuilder = Settings.builder().put(request);
        Settings.Builder effective = Settings.builder();

        additionalSettings.applyTo(Settings.builder().put(template).put(request).build(), effective, requestBuilder);
        effective.put(requestBuilder.build());

        assertThat(effective.build().get("index.a"), nullValue());
    }

    public void testApplyToRemovesOverruledSettingsFromTheRequestBuilder() {
        var additionalSettings = collect(
            List.of(
                provider(Settings.builder().put("index.overruled", "overruling").put("index.overruled_null", "something").build(), true),
                provider(Settings.builder().put("index.default", "provided").build(), false)
            )
        );
        Settings.Builder request = Settings.builder()
            .put("index.overruled", "requested")
            .put("index.default", "requested")
            .put("index.other", "requested")
            .putNull("index.overruled_null");
        Settings.Builder effective = Settings.builder();

        additionalSettings.applyTo(request.build(), effective, request);

        assertThat(request.build(), equalTo(Settings.builder().put("index.default", "requested").put("index.other", "requested").build()));
        assertThat(
            effective.build(),
            equalTo(
                Settings.builder()
                    .put("index.overruled", "overruling")
                    .put("index.default", "requested")
                    .put("index.other", "requested")
                    .put("index.overruled_null", "something")
                    .build()
            )
        );
    }

    public void testApplyToLeavesTheRequestBuilderUntouchedWhenNothingIsOverruled() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "provided").build(), false)));
        Settings requestSettings = Settings.builder().put("index.a", "requested").putNull("index.b").build();
        Settings.Builder request = Settings.builder().put(requestSettings);

        additionalSettings.applyTo(requestSettings, Settings.builder(), request);

        assertThat(request.build(), equalTo(requestSettings));
    }

    public void testApplyToOverrulingSettingNotConfiguredByTheUser() {
        var additionalSettings = collect(List.of(provider(Settings.builder().put("index.a", "overruling").build(), true)));
        Settings.Builder request = Settings.builder().put("index.b", "requested");
        Settings.Builder effective = Settings.builder();

        additionalSettings.applyTo(request.build(), effective, request);

        assertThat(request.build(), equalTo(Settings.builder().put("index.b", "requested").build()));
        assertThat(effective.build(), equalTo(Settings.builder().put("index.a", "overruling").put("index.b", "requested").build()));
    }

    /**
     * List settings such as {@code index.dimensions} must stay lists when applied, otherwise they get flattened to a single
     * "[a, b]" string and are no longer usable by their consumers (e.g. routing).
     */
    public void testListSettingsStayListsWhenApplied() {
        var additionalSettings = collect(
            List.of(
                provider(Settings.builder().putList("index.provided", "a", "b").build(), false),
                provider(Settings.builder().putList("index.overruling", "c", "d").build(), true)
            )
        );

        Settings effective = applyTo(
            additionalSettings,
            Settings.builder().putList("index.configured", "e", "f").putList("index.overruling", "g")
        );
        assertThat(effective.getAsList("index.provided"), equalTo(List.of("a", "b")));
        assertThat(effective.getAsList("index.overruling"), equalTo(List.of("c", "d")));
        assertThat(effective.getAsList("index.configured"), equalTo(List.of("e", "f")));
    }

    public void testListSettingConfiguredByTheUserWinsOverProvidedList() {
        var additionalSettings = collect(List.of(provider(Settings.builder().putList("index.a", "a", "b").build(), false)));

        Settings effective = applyTo(additionalSettings, Settings.builder().putList("index.a", "x", "y"));
        assertThat(effective.getAsList("index.a"), equalTo(List.of("x", "y")));
    }

    public void testDuplicateProvidedSettingIsRejected() {
        Settings settings = Settings.builder().put("index.a", "provided").build();
        var e = expectThrows(IllegalArgumentException.class, () -> collect(List.of(provider(settings, false), provider(settings, false))));
        assertThat(e.getMessage(), containsString("additional index setting [index.a]"));
    }

    public void testSettingIndexVersionCreatedIsRejected() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current().id()).build();
        var e = expectThrows(IllegalArgumentException.class, () -> collect(List.of(provider(settings, randomBoolean()))));
        assertThat(e.getMessage(), containsString("is not allowed to be set via an IndexSettingProvider"));
    }

    /**
     * When a setting subset is given, providers that cannot influence any of these settings are not invoked at all, so they do not
     * contribute any setting.
     */
    public void testSettingSubsetSkipsProvidersThatDoNotInfluenceIt() {
        var relevantInvocations = new AtomicInteger();
        var irrelevantInvocations = new AtomicInteger();
        var additionalSettings = collect(
            List.of(
                countingProvider(Settings.builder().put("index.a", "a").build(), relevantInvocations),
                countingProvider(Settings.builder().put("index.b", "b").build(), irrelevantInvocations)
            ),
            Set.of("index.a", "index.c")
        );
        assertThat(relevantInvocations.get(), equalTo(1));
        assertThat(irrelevantInvocations.get(), equalTo(0));
        assertThat(additionalSettings.settings(), equalTo(Settings.builder().put("index.a", "a").build()));
    }

    /**
     * The subset filters which providers run, not which settings they contribute: a provider that influences at least one of the
     * requested settings contributes all of its settings.
     */
    public void testSettingSubsetRunsMatchingProviderInFull() {
        var invocations = new AtomicInteger();
        var additionalSettings = collect(
            List.of(countingProvider(Settings.builder().put("index.a", "a").put("index.b", "b").build(), invocations)),
            Set.of("index.a")
        );
        assertThat(invocations.get(), equalTo(1));
        assertThat(additionalSettings.settings(), equalTo(Settings.builder().put("index.a", "a").put("index.b", "b").build()));
    }

    public void testNullSettingSubsetRunsAllProviders() {
        var firstInvocations = new AtomicInteger();
        var secondInvocations = new AtomicInteger();
        var additionalSettings = collect(
            List.of(
                countingProvider(Settings.builder().put("index.a", "a").build(), firstInvocations),
                countingProvider(Settings.builder().put("index.b", "b").build(), secondInvocations)
            ),
            null
        );
        assertThat(firstInvocations.get(), equalTo(1));
        assertThat(secondInvocations.get(), equalTo(1));
        assertThat(additionalSettings.settings(), equalTo(Settings.builder().put("index.a", "a").put("index.b", "b").build()));
    }

    public void testEmptySettingSubsetRunsNoProviders() {
        var invocations = new AtomicInteger();
        var additionalSettings = collect(List.of(countingProvider(Settings.builder().put("index.a", "a").build(), invocations)), Set.of());
        assertThat(invocations.get(), equalTo(0));
        assertThat(additionalSettings.settings(), equalTo(Settings.EMPTY));
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of()));
    }

    /**
     * Only the providers that run are validated, so a conflict with a skipped provider does not fail the collection.
     */
    public void testSkippedProviderDoesNotConflict() {
        Settings settings = Settings.builder().put("index.a", "a").build();
        var additionalSettings = collect(
            List.of(
                provider(settings, false),
                provider(settings, randomBoolean()),
                provider(Settings.builder().put("index.b", "b").build(), false)
            ),
            Set.of("index.b")
        );
        assertThat(additionalSettings.settings(), equalTo(Settings.builder().put("index.b", "b").build()));
    }

    public void testOverrulingSettingsOfSkippedProvidersAreIgnored() {
        var additionalSettings = collect(
            List.of(
                provider(Settings.builder().put("index.a", "overruling").build(), true),
                provider(Settings.builder().put("index.b", "overruling").build(), true)
            ),
            Set.of("index.b")
        );
        assertThat(additionalSettings.overrulingSettings(), equalTo(Set.of("index.b")));
        Settings effective = applyTo(additionalSettings, Settings.builder().put("index.a", "configured").put("index.b", "configured"));
        assertThat(effective.get("index.a"), equalTo("configured"));
        assertThat(effective.get("index.b"), equalTo("overruling"));
    }

    private static Settings applyTo(IndexSettingProviders.AdditionalSettings additionalSettings, Settings.Builder resolvedSettings) {
        return additionalSettings.applyTo(resolvedSettings.build());
    }

    private static IndexSettingProviders.AdditionalSettings collect(List<IndexSettingProvider> providers) {
        return IndexSettingProviders.collectAdditionalSettings(
            providers,
            "index",
            null,
            null,
            ProjectMetadata.builder(randomProjectIdOrDefault()).build(),
            Instant.now(),
            Settings.EMPTY,
            List.of(),
            IndexVersion.current()
        );
    }

    private static IndexSettingProviders.AdditionalSettings collect(List<IndexSettingProvider> providers, Set<String> settingSubset) {
        return IndexSettingProviders.collectAdditionalSettings(
            providers,
            "index",
            null,
            null,
            ProjectMetadata.builder(randomProjectIdOrDefault()).build(),
            Instant.now(),
            Settings.EMPTY,
            List.of(),
            IndexVersion.current(),
            settingSubset
        );
    }

    /**
     * A provider that always contributes the given settings and counts how often it was invoked.
     */
    private static IndexSettingProvider countingProvider(Settings settings, AtomicInteger invocations) {
        IndexSettingProvider delegate = provider(settings, false);
        return new IndexSettingProvider() {
            @Override
            public void provideAdditionalSettings(
                String indexName,
                String dataStreamName,
                IndexMode templateIndexMode,
                ProjectMetadata projectMetadata,
                Instant resolvedAt,
                Settings indexTemplateAndCreateRequestSettings,
                List<CompressedXContent> combinedTemplateMappings,
                IndexVersion indexVersion,
                Settings.Builder additionalSettings
            ) {
                invocations.incrementAndGet();
                delegate.provideAdditionalSettings(
                    indexName,
                    dataStreamName,
                    templateIndexMode,
                    projectMetadata,
                    resolvedAt,
                    indexTemplateAndCreateRequestSettings,
                    combinedTemplateMappings,
                    indexVersion,
                    additionalSettings
                );
            }

            @Override
            public Set<String> affectedSettings() {
                return delegate.affectedSettings();
            }
        };
    }

    /**
     * A provider that always contributes the given settings.
     */
    private static IndexSettingProvider provider(Settings settings, boolean overrules) {
        return new IndexSettingProvider() {
            @Override
            public void provideAdditionalSettings(
                String indexName,
                String dataStreamName,
                IndexMode templateIndexMode,
                ProjectMetadata projectMetadata,
                Instant resolvedAt,
                Settings indexTemplateAndCreateRequestSettings,
                List<CompressedXContent> combinedTemplateMappings,
                IndexVersion indexVersion,
                Settings.Builder additionalSettings
            ) {
                additionalSettings.put(settings);
            }

            @Override
            public Set<String> affectedSettings() {
                return settings.keySet();
            }

            @Override
            public boolean overrulesTemplateAndRequestSettings() {
                return overrules;
            }
        };
    }
}
