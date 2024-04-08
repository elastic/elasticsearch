/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.tasks.TaskResultsService.TASKS_FEATURE_NAME;
import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SystemIndicesTests extends ESTestCase {
    private static final String OPTIONAL_UPGRADE_SUFFIX_REGEX = "(" + SystemIndices.UPGRADED_INDEX_SUFFIX + ")?";

    public void testBasicOverlappingPatterns() {
        SystemIndexDescriptor broadPattern = SystemIndexDescriptorUtils.createUnmanaged(".a*c*", "test");
        SystemIndexDescriptor notOverlapping = SystemIndexDescriptorUtils.createUnmanaged(".bbbddd*", "test");
        SystemIndexDescriptor overlapping1 = SystemIndexDescriptorUtils.createUnmanaged(".ac*", "test");
        SystemIndexDescriptor overlapping2 = SystemIndexDescriptorUtils.createUnmanaged(".aaaabbbccc*", "test");
        SystemIndexDescriptor overlapping3 = SystemIndexDescriptorUtils.createUnmanaged(".aaabb*cccddd*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String broadPatternSource = "AAA" + randomAlphaOfLength(5);
        String otherSource = "ZZZ" + randomAlphaOfLength(6);
        Map<String, SystemIndices.Feature> descriptors = new HashMap<>();
        descriptors.put(broadPatternSource, new SystemIndices.Feature(broadPatternSource, "test feature", List.of(broadPattern)));
        descriptors.put(
            otherSource,
            new SystemIndices.Feature(otherSource, "test 2", List.of(notOverlapping, overlapping1, overlapping2, overlapping3))
        );

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> SystemIndices.checkForOverlappingPatterns(descriptors)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "a system index descriptor ["
                    + broadPattern
                    + "] from ["
                    + broadPatternSource
                    + "] overlaps with other system index descriptors:"
            )
        );
        String fromPluginString = " from [" + otherSource + "]";
        assertThat(exception.getMessage(), containsString(overlapping1.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping2.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping3.toString() + fromPluginString));
        assertThat(exception.getMessage(), not(containsString(notOverlapping.toString())));

        IllegalStateException constructorException = expectThrows(
            IllegalStateException.class,
            () -> new SystemIndices(List.copyOf(descriptors.values()))
        );
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testComplexOverlappingPatterns() {
        // These patterns are slightly more complex to detect because pattern1 does not match pattern2 and vice versa
        SystemIndexDescriptor pattern1 = SystemIndexDescriptorUtils.createUnmanaged(".a*c", "test");
        SystemIndexDescriptor pattern2 = SystemIndexDescriptorUtils.createUnmanaged(".ab*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String source1 = "AAA" + randomAlphaOfLength(5);
        String source2 = "ZZZ" + randomAlphaOfLength(6);
        Map<String, SystemIndices.Feature> descriptors = new HashMap<>();
        descriptors.put(source1, new SystemIndices.Feature(source1, "test", List.of(pattern1)));
        descriptors.put(source2, new SystemIndices.Feature(source2, "test", List.of(pattern2)));

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> SystemIndices.checkForOverlappingPatterns(descriptors)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "a system index descriptor [" + pattern1 + "] from [" + source1 + "] overlaps with other system index descriptors:"
            )
        );
        assertThat(exception.getMessage(), containsString(pattern2.toString() + " from [" + source2 + "]"));

        IllegalStateException constructorException = expectThrows(
            IllegalStateException.class,
            () -> new SystemIndices(List.copyOf(descriptors.values()))
        );
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testBuiltInSystemIndices() {
        SystemIndices systemIndices = new SystemIndices(List.of());
        assertTrue(systemIndices.isSystemIndex(".tasks"));
        assertTrue(systemIndices.isSystemIndex(".tasks1"));
        assertTrue(systemIndices.isSystemIndex(".tasks-old"));
        assertTrue(systemIndices.isSystemIndex(".synonyms"));
    }

    public void testPluginCannotOverrideBuiltInSystemIndex() {
        List<SystemIndices.Feature> pluginMap = List.of(
            new SystemIndices.Feature(
                TASKS_FEATURE_NAME,
                "test",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(TASK_INDEX + "*", "Task" + " Result Index"))
            )
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SystemIndices(pluginMap));
        assertThat(e.getMessage(), containsString("plugin or module attempted to define the same source"));
    }

    public void testPatternWithSimpleRange() {

        final SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "test",
                    "test feature",
                    List.of(SystemIndexDescriptorUtils.createUnmanaged(".test-[abc]" + OPTIONAL_UPGRADE_SUFFIX_REGEX, ""))
                )
            )
        );

        assertThat(systemIndices.isSystemIndex(".test-a"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-b"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-c"), equalTo(true));

        assertThat(systemIndices.isSystemIndex(".test-aa"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-d"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-="), equalTo(false));
    }

    public void testPatternWithSimpleRangeAndRepeatOperator() {
        final SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "test",
                    "test feature",
                    List.of(SystemIndexDescriptorUtils.createUnmanaged(".test-[a]+" + OPTIONAL_UPGRADE_SUFFIX_REGEX, ""))
                )
            )
        );

        assertThat(systemIndices.isSystemIndex(".test-a"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-aa"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-aaa"), equalTo(true));

        assertThat(systemIndices.isSystemIndex(".test-b"), equalTo(false));
    }

    public void testPatternWithComplexRange() {
        final SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "test",
                    "test feature",
                    List.of(SystemIndexDescriptorUtils.createUnmanaged(".test-[a-c]" + OPTIONAL_UPGRADE_SUFFIX_REGEX, ""))
                )
            )
        );

        assertThat(systemIndices.isSystemIndex(".test-a"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-b"), equalTo(true));
        assertThat(systemIndices.isSystemIndex(".test-c"), equalTo(true));

        assertThat(systemIndices.isSystemIndex(".test-aa"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-d"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-"), equalTo(false));
        assertThat(systemIndices.isSystemIndex(".test-="), equalTo(false));
    }

    public void testOverlappingDescriptorsWithRanges() {
        String source1 = "source1";
        String source2 = "source2";

        SystemIndexDescriptor pattern1 = SystemIndexDescriptorUtils.createUnmanaged(".test-[ab]*", "");
        SystemIndexDescriptor pattern2 = SystemIndexDescriptorUtils.createUnmanaged(".test-a*", "");

        Map<String, SystemIndices.Feature> descriptors = new HashMap<>();
        descriptors.put(source1, new SystemIndices.Feature(source1, "source 1", List.of(pattern1)));
        descriptors.put(source2, new SystemIndices.Feature(source2, "source 2", List.of(pattern2)));

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> SystemIndices.checkForOverlappingPatterns(descriptors)
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "a system index descriptor [" + pattern1 + "] from [" + source1 + "] overlaps with other system index descriptors:"
            )
        );

        assertThat(exception.getMessage(), containsString(pattern2.toString() + " from [" + source2 + "]"));
    }

    public void testPatternsWithNoRoomForUpgradeSuffix() {
        final SystemIndexDescriptor endsWithNumbersOnly = SystemIndexDescriptorUtils.createUnmanaged(
            ".desc[0-9]+",
            "can only end with numbers"
        );
        final SystemIndexDescriptor concreteIndex = SystemIndexDescriptorUtils.createUnmanaged(".concrete", "concrete index");
        final SystemIndexDescriptor okayDescriptor = SystemIndexDescriptorUtils.createUnmanaged(".okay*", "concrete index");
        final SystemIndexDescriptor endsWithNumbersThenWildcard = SystemIndexDescriptorUtils.createUnmanaged(
            ".desc[0-9]+*",
            "concrete index"
        );

        final Map<String, SystemIndices.Feature> features = new HashMap<>();
        final String firstFeature = "first";
        features.put(
            firstFeature,
            new SystemIndices.Feature(
                firstFeature,
                this.getTestName() + " - " + firstFeature,
                Collections.singletonList(endsWithNumbersOnly)
            )
        );
        final String secondFeature = "second";
        features.put(
            secondFeature,
            new SystemIndices.Feature(secondFeature, this.getTestName() + " - " + secondFeature, List.of(concreteIndex, okayDescriptor))
        );
        final String thirdFeature = "third";
        features.put(
            thirdFeature,
            new SystemIndices.Feature(thirdFeature, this.getTestName() + " - " + thirdFeature, List.of(endsWithNumbersThenWildcard))
        );

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> SystemIndices.ensurePatternsAllowSuffix(features));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString(endsWithNumbersOnly.getIndexPattern()),
                containsString(secondFeature),
                containsString(concreteIndex.getIndexPattern()),
                containsString(firstFeature)
            )
        );
    }

    public void testMappingsVersions() {
        SystemIndexDescriptor unmanaged = SystemIndexDescriptorUtils.createUnmanaged(".unmanaged-*", "unmanaged");
        SystemIndexDescriptor managed = SystemIndexDescriptor.builder()
            .setIndexPattern(".managed-*")
            .setPrimaryIndex(".managed-primary")
            .setVersionMetaKey("version")
            .setOrigin("system")
            .setSettings(Settings.EMPTY)
            .setMappings("""
                  {
                    "_meta": {
                      "version": "8.0.0",
                      "managed_index_mappings_version": 3
                    },
                    "properties": {
                      "name": { "type": "text" }
                    }
                  }
                """)
            .build();

        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature("unmanaged", "unmanaged", List.of(unmanaged)),
                new SystemIndices.Feature("managed", "managed", List.of(managed))
            )
        );

        Map<String, SystemIndexDescriptor.MappingsVersion> mappingsVersions = systemIndices.getMappingsVersions();
        assertThat(mappingsVersions.get(".managed-primary"), notNullValue());
        assertThat(mappingsVersions.get(".managed-primary").version(), equalTo(3));
        assertThat(mappingsVersions.keySet(), not(contains("unmanaged")));
    }
}
