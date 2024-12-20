/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.indices.SystemIndexDescriptor.findDynamicMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SystemIndexDescriptorTests extends ESTestCase {

    private static final int TEST_MAPPINGS_VERSION = 10;
    private static final int TEST_MAPPINGS_PRIOR_VERSION = 5;
    private static final int TEST_MAPPINGS_NONEXISTENT_VERSION = 2;

    private static final String MAPPINGS_FORMAT_STRING = """
        {
          "_doc": {
            "_meta": {
              "%s": %d
            }
          }
        }
        """;

    private static final String MAPPINGS = getVersionedMappings(TEST_MAPPINGS_VERSION);

    /**
     * Tests the various validation rules that are applied when creating a new system index descriptor.
     */
    public void testValidation() {
        {
            Exception ex = expectThrows(
                NullPointerException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern(null)
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
            );
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern("")
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
            );
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern(".")
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
            );
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern(randomAlphaOfLength(10))
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
            );
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern(".*")
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
            );
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder()
                    .setIndexPattern(".*" + randomAlphaOfLength(10))
                    .setDescription(randomAlphaOfLength(5))
                    .setType(Type.INTERNAL_UNMANAGED)
                    .build()
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
        String json = """
            {
              "foo": {
                "bar": {
                  "dynamic": false
                },
                "baz": {
                  "dynamic": true
                }
              }
            }""";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(true));
    }

    /**
     * Check that a system index descriptor correctly identifies the absence of a dynamic mapping when none are present.
     */
    public void testFindDynamicMappingsWithoutDynamicMapping() {
        String json = """
            { "foo": { "bar": { "dynamic": false } } }""";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(false));
    }

    public void testPriorSystemIndexDescriptorValidation() {
        SystemIndexDescriptor prior = priorSystemIndexDescriptorBuilder().build();

        // same minimum node version
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setPriorSystemIndexDescriptors(List.of(prior)).build()
        );
        assertThat(iae.getMessage(), containsString("same mappings version"));

        // different min version but prior is after latest!
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION - 1))
                .setPriorSystemIndexDescriptors(List.of(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("has mappings version [10] which is after [9]"));

        // prior has another prior!
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 2))
                .setPriorSystemIndexDescriptors(
                    List.of(
                        SystemIndexDescriptor.builder()
                            .setIndexPattern(".system*")
                            .setDescription("system stuff")
                            .setPrimaryIndex(".system-1")
                            .setAliasName(".system")
                            .setType(Type.INTERNAL_MANAGED)
                            .setSettings(Settings.EMPTY)
                            .setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 1))
                            .setOrigin("system")
                            .setPriorSystemIndexDescriptors(List.of(prior))
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
                .setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 1))
                .setPriorSystemIndexDescriptors(List.of(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("index pattern must be the same"));

        // different primary index
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setPrimaryIndex(".system-2")
                .setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 1))
                .setPriorSystemIndexDescriptors(List.of(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("primary index must be the same"));

        // different alias
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> priorSystemIndexDescriptorBuilder().setAliasName(".system1")
                .setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 1))
                .setPriorSystemIndexDescriptors(List.of(prior))
                .build()
        );
        assertThat(iae.getMessage(), containsString("alias name must be the same"));

        // success!
        assertNotNull(
            priorSystemIndexDescriptorBuilder().setMappings(getVersionedMappings(TEST_MAPPINGS_VERSION + 1))
                .setPriorSystemIndexDescriptors(List.of(prior))
                .build()
        );
    }

    private static String getVersionedMappings(int version) {
        return Strings.format(MAPPINGS_FORMAT_STRING, SystemIndexDescriptor.VERSION_META_KEY, version);
    }

    public void testGetDescriptorCompatibleWith() {
        final SystemIndexDescriptor prior = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setSettings(Settings.EMPTY)
            .setMappings(getVersionedMappings(TEST_MAPPINGS_PRIOR_VERSION))
            .setOrigin("system")
            .build();
        final SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setSettings(Settings.EMPTY)
            .setMappings(MAPPINGS)
            .setOrigin("system")
            .setPriorSystemIndexDescriptors(List.of(prior))
            .build();

        SystemIndexDescriptor compat = descriptor.getDescriptorCompatibleWith(descriptor.getMappingsVersion());
        assertSame(descriptor, compat);

        assertNull(descriptor.getDescriptorCompatibleWith(new SystemIndexDescriptor.MappingsVersion(TEST_MAPPINGS_NONEXISTENT_VERSION, 1)));

        SystemIndexDescriptor.MappingsVersion priorToMinMappingsVersion = new SystemIndexDescriptor.MappingsVersion(
            TEST_MAPPINGS_PRIOR_VERSION,
            1
        );
        compat = descriptor.getDescriptorCompatibleWith(priorToMinMappingsVersion);
        assertSame(prior, compat);

        compat = descriptor.getDescriptorCompatibleWith(
            new SystemIndexDescriptor.MappingsVersion(randomIntBetween(TEST_MAPPINGS_PRIOR_VERSION, TEST_MAPPINGS_VERSION - 1), 1)
        );
        assertSame(prior, compat);
    }

    public void testSystemIndicesMustBeHidden() {
        SystemIndexDescriptor.Builder builder = SystemIndexDescriptor.builder()
            .setIndexPattern(".system*")
            .setDescription("system stuff")
            .setPrimaryIndex(".system-1")
            .setAliasName(".system")
            .setType(Type.INTERNAL_MANAGED)
            .setMappings(MAPPINGS)
            .setOrigin("system");

        builder.setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertThat(e.getMessage(), equalTo("System indices must have index.hidden set to true."));
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
            .setOrigin("system");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertThat(e.getMessage(), equalTo("Descriptor index format does not match index format in managed settings"));
    }

    public void testUnmanagedIndexMappingsVersion() {
        SystemIndexDescriptor indexDescriptor = SystemIndexDescriptor.builder()
            .setIndexPattern(".unmanaged-*")
            .setDescription("an unmanaged system index")
            .setType(Type.INTERNAL_UNMANAGED)
            .build();

        IllegalStateException e = expectThrows(IllegalStateException.class, indexDescriptor::getMappingsVersion);

        assertThat(e.getMessage(), containsString("is not managed so there are no mappings or version"));
    }

    // test mapping versions can't be negative
    public void testNegativeMappingsVersion() {
        int negativeVersion = randomIntBetween(Integer.MIN_VALUE, -1);
        String mappings = Strings.format(MAPPINGS_FORMAT_STRING, SystemIndexDescriptor.VERSION_META_KEY, negativeVersion);

        SystemIndexDescriptor.Builder builder = priorSystemIndexDescriptorBuilder().setMappings(mappings);

        AssertionError e = expectThrows(AssertionError.class, builder::build);

        assertThat(e.getMessage(), equalTo("The mappings version must not be negative"));
    }

    public void testMappingsVersionCompareTo() {
        SystemIndexDescriptor.MappingsVersion mv1 = new SystemIndexDescriptor.MappingsVersion(1, randomInt(20));
        SystemIndexDescriptor.MappingsVersion mv2 = new SystemIndexDescriptor.MappingsVersion(2, randomInt(20));

        NullPointerException e = expectThrows(NullPointerException.class, () -> mv1.compareTo(null));
        assertThat(e.getMessage(), equalTo("Cannot compare null MappingsVersion"));

        assertThat(mv1.compareTo(mv2), equalTo(-1));
        assertThat(mv1.compareTo(mv1), equalTo(0));
        assertThat(mv2.compareTo(mv1), equalTo(1));
    }

    public void testHashesIgnoreMappingMetadata() {
        String mappingFormatString = """
            {
              "_doc": {
                "_meta": {
                  "%s": %d
                }
              },
              "properties": {
                "age":    { "type": "integer" },
                "email":  { "type": "keyword"  },
                "name":   { "type": "text"  }
              }
            }
            """;

        String mappings1 = Strings.format(mappingFormatString, SystemIndexDescriptor.VERSION_META_KEY, randomIntBetween(1, 10));
        String mappings2 = Strings.format(mappingFormatString, SystemIndexDescriptor.VERSION_META_KEY, randomIntBetween(11, 20));

        SystemIndexDescriptor descriptor1 = priorSystemIndexDescriptorBuilder().setMappings(mappings1).build();
        SystemIndexDescriptor descriptor2 = priorSystemIndexDescriptorBuilder().setMappings(mappings2).build();

        assertThat(descriptor1.getMappingsVersion().hash(), equalTo(descriptor2.getMappingsVersion().hash()));
        assertThat(descriptor1.getMappingsVersion().version(), not(equalTo(descriptor2.getMappingsVersion().version())));
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
            .setOrigin("system");
    }
}
