/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.indices.SystemIndexDescriptor.findDynamicMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SystemIndexDescriptorTests extends ESTestCase {

    private static final String MAPPINGS = "{ \"_doc\": { \"_meta\": { \"version\": \"7.4.0\" } } }";

    /**
     * Tests the various validation rules that are applied when creating a new system index descriptor.
     */
    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class, () -> new SystemIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            final String primaryIndex = randomAlphaOfLength(5);
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder().setIndexPattern("." + primaryIndex).setPrimaryIndex(primaryIndex).build()
            );
            assertThat(
                ex.getMessage(),
                equalTo("system primary index provided as [" + primaryIndex + "] but must start with the character [.]")
            );
        }
        {
            final String primaryIndex = "." + randomAlphaOfLength(5) + "*";
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder().setIndexPattern("." + randomAlphaOfLength(5)).setPrimaryIndex(primaryIndex).build()
            );
            assertThat(
                ex.getMessage(),
                equalTo("system primary index provided as [" + primaryIndex + "] but cannot contain special characters or patterns")
            );
        }
    }

    /**
     * Check that a system index descriptor correctly identifies the presence of a dynamic mapping when once is present.
     */
    public void testFindDynamicMappingsWithDynamicMapping() {
        String json = "{"
            + "  \"foo\": {"
            + "    \"bar\": {"
            + "      \"dynamic\": false"
            + "    },"
            + "    \"baz\": {"
            + "      \"dynamic\": true"
            + "    }"
            + "  }"
            + "}";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(true));
    }

    /**
     * Check that a system index descriptor correctly identifies the absence of a dynamic mapping when none are present.
     */
    public void testFindDynamicMappingsWithoutDynamicMapping() {
        String json = "{ \"foo\": { \"bar\": { \"dynamic\": false } } }";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(false));
    }

    public void testPriorSystemIndexDescriptorValidation() {
        SystemIndexDescriptor prior = priorSystemIndexDescriptorBuilder().build();

        // same minimum node version
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setPriorSystemIndexDescriptors(singletonList(prior)).build()
        );
        assertThat(iae.getMessage(), containsString("same minimum node version"));

        // different min version but prior is after latest!
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setMinimumNodeVersion(Version.fromString("6.8.0"))
                .setPriorSystemIndexDescriptors(singletonList(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("has minimum node version [7.0.0] which is after [6.8.0]"));

        // prior has another prior!
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setMinimumNodeVersion(Version.V_7_5_0)
                .setPriorSystemIndexDescriptors(
                    singletonList(
                        SystemIndexDescriptor.builder()
                            .setIndexPattern(".system*")
                            .setDescription("system stuff")
                            .setPrimaryIndex(".system-1")
                            .setAliasName(".system")
                            .setType(Type.INTERNAL_MANAGED)
                            .setSettings(Settings.EMPTY)
                            .setMappings(MAPPINGS)
                            .setVersionMetaKey("version")
                            .setOrigin("system")
                            .setMinimumNodeVersion(Version.V_7_4_1)
                            .setPriorSystemIndexDescriptors(singletonList(prior))
                            .build()
                    )
                )
                .build()
        );
        assertThat(iae.getMessage(), containsString("has its own prior descriptors"));

        // different index patterns
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setIndexPattern(".system1*")
                .setMinimumNodeVersion(Version.V_7_5_0)
                .setPriorSystemIndexDescriptors(singletonList(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("index pattern must be the same"));

        // different primary index
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setPrimaryIndex(".system-2")
                .setMinimumNodeVersion(Version.V_7_5_0)
                .setPriorSystemIndexDescriptors(singletonList(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("primary index must be the same"));

        // different alias
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setAliasName(".system1")
                .setMinimumNodeVersion(Version.V_7_5_0)
                .setPriorSystemIndexDescriptors(singletonList(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("alias name must be the same"));

        // success!
        assertNotNull(
            priorSystemIndexDescriptorBuilder().setMinimumNodeVersion(Version.V_7_5_0)
                .setPriorSystemIndexDescriptors(singletonList(prior))
                .build()
        );
    }

    public void testGetDescriptorCompatibleWith() {
        final String mappings = "{ \"_doc\": { \"_meta\": { \"version\": \"7.4.0\" } } }";
        final SystemIndexDescriptor prior = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setSettings(Settings.EMPTY)
            .setMappings(mappings)
            .setVersionMetaKey("version")
            .setOrigin("system")
            .build();
        final SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setSettings(Settings.EMPTY)
            .setMappings(mappings)
            .setVersionMetaKey("version")
            .setOrigin("system")
            .setMinimumNodeVersion(Version.V_7_0_0)
            .setPriorSystemIndexDescriptors(singletonList(prior))
            .build();

        SystemIndexDescriptor compat = descriptor.getDescriptorCompatibleWith(Version.CURRENT);
        assertSame(descriptor, compat);

        assertNull(descriptor.getDescriptorCompatibleWith(Version.fromString("6.7.0")));

        compat = descriptor.getDescriptorCompatibleWith(Version.V_7_0_0);
        assertSame(descriptor, compat);

        Version priorToMin = VersionUtils.getPreviousVersion(descriptor.getMinimumNodeVersion());
        compat = descriptor.getDescriptorCompatibleWith(priorToMin);
        assertSame(prior, compat);

        compat = descriptor.getDescriptorCompatibleWith(
            VersionUtils.randomVersionBetween(random(), prior.getMinimumNodeVersion(), priorToMin)
        );
        assertSame(prior, compat);
    }

    public void testSystemIndicesCannotAlsoBeHidden() {
        SystemIndexDescriptor.Builder builder = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setMappings(MAPPINGS)
            .setVersionMetaKey("version")
            .setOrigin("system");

        builder.setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertThat(e.getMessage(), equalTo("System indices cannot have index.hidden set to true."));
    }

    public void testSpecialCharactersAreReplacedWhenConvertingToAutomaton() {
        CharacterRunAutomaton automaton = new CharacterRunAutomaton(
            SystemIndexDescriptor.buildAutomaton(".system-index*", ".system-alias")
        );

        // None of these should match, ever.
        assertFalse(automaton.run(".my-system-index"));
        assertFalse(automaton.run("my.system-index"));
        assertFalse(automaton.run("some-other-index"));

        // These should only fail if the trailing `*` doesn't get properly replaced with `.*`
        assertTrue("if the trailing * isn't replaced, suffixes won't match properly", automaton.run(".system-index-1"));
        assertTrue("if the trailing * isn't replaced, suffixes won't match properly", automaton.run(".system-index-asdf"));

        // These should only fail if the leading `.` doesn't get properly replaced with `\\.`
        assertFalse("if the leading dot isn't replaced, it can match date math", automaton.run("<system-index-{now/d}>"));
        assertFalse("if the leading dot isn't replaced, it can match any single-char prefix", automaton.run("Osystem-index"));
        assertFalse("the leading dot got dropped", automaton.run("system-index-1"));
    }

    public void testManagedSystemIndexMustHaveMatchingIndexFormat() {
        SystemIndexDescriptor.Builder builder = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setMappings(MAPPINGS)
            .setSettings(Settings.builder().put("index.format", 5).build())
            .setIndexFormat(0)
            .setVersionMetaKey("version")
            .setOrigin("system");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertThat(e.getMessage(), equalTo("Descriptor index format does not match index format in managed settings"));
    }

    private SystemIndexDescriptor.Builder priorSystemIndexDescriptorBuilder() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setSettings(Settings.EMPTY)
            .setMappings(MAPPINGS)
            .setVersionMetaKey("version")
            .setOrigin("system")
            .setMinimumNodeVersion(Version.V_7_0_0);
    }
}
