/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class ProjectSettingsUpdaterTests extends ESTestCase {

    private static final Setting<Float> SETTING_A = Setting.floatSetting(
        "project.setting_a",
        0.55f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );
    private static final Setting<Float> SETTING_B = Setting.floatSetting(
        "project.setting_b",
        0.1f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );

    private static ProjectMetadata projectWithSettings(Settings settings) {
        ProjectId projectId = randomUniqueProjectId();
        return ProjectMetadata.builder(projectId).settings(settings).build();
    }

    public void testUpdateSetting() {
        AtomicReference<Float> valueA = new AtomicReference<>();
        AtomicReference<Float> valueB = new AtomicReference<>();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(SETTING_A, SETTING_B));
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_A, valueA::set);
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_B, valueB::set);
        SettingsUpdater updater = new SettingsUpdater(projectScopedSettings);
        ProjectMetadata projectMetadata = projectWithSettings(
            Settings.builder().put(SETTING_A.getKey(), 1.5).put(SETTING_B.getKey(), 2.5).build()
        );
        ProjectMetadata updatedProjectMetadata = updater.updateProjectSettings(
            projectMetadata,
            Settings.builder().put(SETTING_A.getKey(), 0.5).build(),
            logger
        );
        assertNotSame(updatedProjectMetadata, projectMetadata);
        assertEquals(SETTING_A.get(updatedProjectMetadata.settings()), 0.4, 0.1);
        assertEquals(SETTING_B.get(updatedProjectMetadata.settings()), 2.5, 0.1);

        updatedProjectMetadata = updater.updateProjectSettings(projectMetadata, Settings.builder().putNull("project.*").build(), logger);
        assertEquals(SETTING_A.get(updatedProjectMetadata.settings()), 0.55, 0.1);
        assertEquals(SETTING_B.get(updatedProjectMetadata.settings()), 0.1, 0.1);

        assertNull("updater only does a dryRun", valueA.get());
        assertNull("updater only does a dryRun", valueB.get());
    }

    public void testAllOrNothing() {
        AtomicReference<Float> valueA = new AtomicReference<>();
        AtomicReference<Float> valueB = new AtomicReference<>();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(SETTING_A, SETTING_B));
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_A, valueA::set);
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_B, valueB::set);
        SettingsUpdater updater = new SettingsUpdater(projectScopedSettings);
        ProjectMetadata projectMetadata = projectWithSettings(
            Settings.builder().put(SETTING_A.getKey(), 1.5).put(SETTING_B.getKey(), 2.5).build()
        );

        try {
            updater.updateProjectSettings(
                projectMetadata,
                Settings.builder().put(SETTING_A.getKey(), "not a float").put(SETTING_B.getKey(), 1.0f).build(),
                logger
            );
            fail("all or nothing");
        } catch (IllegalArgumentException ex) {
            logger.info("", ex);
            assertEquals("Failed to parse value [not a float] for setting [project.setting_a]", ex.getMessage());
        }
        assertNull("updater only does a dryRun", valueA.get());
        assertNull("updater only does a dryRun", valueB.get());
    }

    public void testDeprecationLogging() {
        Setting<String> deprecatedSetting = Setting.simpleString(
            "deprecated.setting",
            Property.Dynamic,
            Property.NodeScope,
            Property.ProjectScope,
            Property.DeprecatedWarning
        );
        final Settings settings = Settings.builder().put("deprecated.setting", "foo").build();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(deprecatedSetting, SETTING_A));
        projectScopedSettings.addSettingsUpdateConsumer(deprecatedSetting, s -> {});
        final SettingsUpdater settingsUpdater = new SettingsUpdater(projectScopedSettings);
        ProjectMetadata projectMetadata = projectWithSettings(settings);

        final Settings toApplyDebug = Settings.builder().put(SETTING_A.getKey(), 1.0f).build();
        final ProjectMetadata afterDebug = settingsUpdater.updateProjectSettings(projectMetadata, toApplyDebug, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        final Settings toApplyUnset = Settings.builder().putNull(SETTING_A.getKey()).build();
        final ProjectMetadata afterUnset = settingsUpdater.updateProjectSettings(afterDebug, toApplyUnset, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        // we also check that if no settings are changed, deprecation logging still occurs
        settingsUpdater.updateProjectSettings(afterUnset, toApplyUnset, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });
    }

    public void testUpdateWithUnknownAndSettings() {
        // we will randomly apply some new dynamic persistent and transient settings
        final int numberOfDynamicSettings = randomIntBetween(1, 8);
        final List<Setting<String>> dynamicSettings = new ArrayList<>(numberOfDynamicSettings);
        for (int i = 0; i < numberOfDynamicSettings; i++) {
            final Setting<String> dynamicSetting = Setting.simpleString(
                "dynamic.setting" + i,
                Property.Dynamic,
                Property.NodeScope,
                Property.ProjectScope
            );
            dynamicSettings.add(dynamicSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingSettings = Settings.builder();

        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (randomBoolean()) {
                existingSettings.put(dynamicSetting.getKey(), "existing_value");
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            existingSettings.put(invalidSetting.getKey(), "value");
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            existingSettings.put(unknownSetting.getKey(), "value");
        }

        // register all the known settings (note that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            Stream.of(SETTING_A, SETTING_B),
            Stream.concat(dynamicSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, knownSettings);
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            projectScopedSettings.addSettingsUpdateConsumer(dynamicSetting, s -> {});
        }
        final SettingsUpdater settingsUpdater = new SettingsUpdater(projectScopedSettings);
        ProjectMetadata projectMetadata = projectWithSettings(existingSettings.build());

        // prepare the dynamic settings update
        final Settings.Builder toApply = Settings.builder();
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (randomBoolean()) {
                toApply.put(dynamicSetting.getKey(), "new_value");
            }
        }

        final ProjectMetadata afterUpdate = settingsUpdater.updateProjectSettings(projectMetadata, toApply.build(), logger);

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            assertThat(afterUpdate.settings().keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey()));
            assertThat(afterUpdate.settings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            assertThat(afterUpdate.settings().keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey()));
            assertThat(afterUpdate.settings().keySet(), not(hasItem(unknownSetting.getKey())));
        }

        // the dynamic settings should be applied
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (toApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(afterUpdate.settings().keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(afterUpdate.settings().get(dynamicSetting.getKey()), equalTo("new_value"));
            } else {
                if (existingSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(afterUpdate.settings().keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(afterUpdate.settings().get(dynamicSetting.getKey()), equalTo("existing_value"));
                } else {
                    assertThat(afterUpdate.settings().keySet(), not(hasItem(dynamicSetting.getKey())));
                }
            }
        }
    }

    public void testRemovingArchivedSettingsDoesNotRemoveNonArchivedInvalidOrUnknownSettings() {
        // these are settings that are archived in the cluster state as either persistent or transient settings
        final int numberOfArchivedSettings = randomIntBetween(1, 8);
        final List<Setting<String>> archivedSettings = new ArrayList<>(numberOfArchivedSettings);
        for (int i = 0; i < numberOfArchivedSettings; i++) {
            final Setting<String> archivedSetting = Setting.simpleString("setting", Property.NodeScope, Property.ProjectScope);
            archivedSettings.add(archivedSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingSettings = Settings.builder();

        for (final Setting<String> archivedSetting : archivedSettings) {
            existingSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            existingSettings.put(invalidSetting.getKey(), "value");
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            existingSettings.put(unknownSetting.getKey(), "value");
        }

        // register all the known settings (not that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            Stream.of(SETTING_A, SETTING_B),
            Stream.concat(archivedSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, knownSettings);
        final SettingsUpdater settingsUpdater = new SettingsUpdater(projectScopedSettings);
        final ProjectMetadata projectMetadata = projectWithSettings(existingSettings.build());

        final Settings.Builder toApply = Settings.builder().put("archived.*", (String) null);

        final ProjectMetadata afterUpdate = settingsUpdater.updateProjectSettings(projectMetadata, toApply.build(), logger);

        // existing archived settings are removed
        for (final Setting<String> archivedSetting : archivedSettings) {
            assertThat(afterUpdate.settings().keySet(), not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey())));
        }

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            assertThat(afterUpdate.settings().keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey()));
            assertThat(afterUpdate.settings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            assertThat(afterUpdate.settings().keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey()));
            assertThat(afterUpdate.settings().keySet(), not(hasItem(unknownSetting.getKey())));
        }
    }

    private static List<Setting<String>> unknownSettings(int numberOfUnknownSettings) {
        final List<Setting<String>> unknownSettings = new ArrayList<>(numberOfUnknownSettings);
        for (int i = 0; i < numberOfUnknownSettings; i++) {
            unknownSettings.add(Setting.simpleString("unknown.setting" + i, Property.NodeScope, Property.ProjectScope));
        }
        return unknownSettings;
    }

    private static List<Setting<String>> invalidSettings(int numberOfInvalidSettings) {
        final List<Setting<String>> invalidSettings = new ArrayList<>(numberOfInvalidSettings);
        for (int i = 0; i < numberOfInvalidSettings; i++) {
            invalidSettings.add(randomBoolean() ? invalidInIsolationSetting(i) : invalidWithDependenciesSetting(i));
        }
        return invalidSettings;
    }

    private static Setting<String> invalidInIsolationSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {
                throw new IllegalArgumentException("Invalid in isolation setting");
            }

        }, Property.NodeScope, Property.ProjectScope);
    }

    private static Setting<String> invalidWithDependenciesSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                throw new IllegalArgumentException("Invalid with dependencies setting");
            }

        }, Property.NodeScope, Property.ProjectScope);
    }

    private static class FooLowSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {}

        @Override
        public void validate(final Integer low, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_HIGH) && low > (int) settings.get(SETTING_FOO_HIGH)) {
                throw new IllegalArgumentException("[low]=" + low + " is higher than [high]=" + settings.get(SETTING_FOO_HIGH));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_HIGH);
            return settings.iterator();
        }

    }

    private static class FooHighSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {

        }

        @Override
        public void validate(final Integer high, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_LOW) && high < (int) settings.get(SETTING_FOO_LOW)) {
                throw new IllegalArgumentException("[high]=" + high + " is lower than [low]=" + settings.get(SETTING_FOO_LOW));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_LOW);
            return settings.iterator();
        }

    }

    private static final Setting<Integer> SETTING_FOO_LOW = new Setting<>(
        "foo.low",
        "10",
        Integer::valueOf,
        new FooLowSettingValidator(),
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );
    private static final Setting<Integer> SETTING_FOO_HIGH = new Setting<>(
        "foo.high",
        "100",
        Integer::valueOf,
        new FooHighSettingValidator(),
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );

    public void testUpdateOfValidationDependentSettings() {
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(
            Settings.EMPTY,
            new HashSet<>(asList(SETTING_FOO_LOW, SETTING_FOO_HIGH))
        );
        final SettingsUpdater updater = new SettingsUpdater(projectScopedSettings);
        ProjectMetadata projectMetadata = projectWithSettings(Settings.EMPTY);

        projectMetadata = updater.updateProjectSettings(
            projectMetadata,
            Settings.builder().put(SETTING_FOO_LOW.getKey(), 20).build(),
            logger
        );
        assertThat(projectMetadata.settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));

        projectMetadata = updater.updateProjectSettings(
            projectMetadata,
            Settings.builder().put(SETTING_FOO_HIGH.getKey(), 40).build(),
            logger
        );
        assertThat(projectMetadata.settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));
        assertThat(projectMetadata.settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        projectMetadata = updater.updateProjectSettings(
            projectMetadata,
            Settings.builder().put(SETTING_FOO_LOW.getKey(), 5).build(),
            logger
        );
        assertThat(projectMetadata.settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(projectMetadata.settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        projectMetadata = updater.updateProjectSettings(
            projectMetadata,
            Settings.builder().put(SETTING_FOO_HIGH.getKey(), 8).build(),
            logger
        );
        assertThat(projectMetadata.settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(projectMetadata.settings().get(SETTING_FOO_HIGH.getKey()), equalTo("8"));

        final ProjectMetadata finalProjectMetadata = projectMetadata;
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateProjectSettings(finalProjectMetadata, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 2).build(), logger)
        );

        assertThat(
            exception.getMessage(),
            either(equalTo("[high]=2 is lower than [low]=5")).or(equalTo("[low]=5 is higher than [high]=2"))
        );
    }

}
