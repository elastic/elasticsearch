/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;

public class IndexDeprecationChecksTests extends ESTestCase {

    private static void assertSettingsAndIssue(String key, String value, DeprecationIssue expected) {
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(settings(Version.V_5_6_0)
                .put(key, value))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testCoerceBooleanDeprecation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject(); {
            mapping.startObject("properties"); {
                mapping.startObject("my_boolean"); {
                    mapping.field("type", "boolean");
                }
                mapping.endObject();
                mapping.startObject("my_object"); {
                    mapping.startObject("properties"); {
                        mapping.startObject("my_inner_boolean"); {
                            mapping.field("type", "boolean");
                        }
                        mapping.endObject();
                        mapping.startObject("my_text"); {
                            mapping.field("type", "text");
                            mapping.startObject("fields"); {
                                mapping.startObject("raw"); {
                                    mapping.field("type", "boolean");
                                }
                                mapping.endObject();
                            }
                            mapping.endObject();
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .putMapping("testBooleanCoercion", Strings.toString(mapping))
            .settings(settings(Version.V_5_6_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.INFO,
            "Coercion of boolean fields",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                "breaking_60_mappings_changes.html#_coercion_of_boolean_fields",
            "[[type: testBooleanCoercion, field: my_boolean], [type: testBooleanCoercion, field: my_inner_boolean]," +
                " [type: testBooleanCoercion, field: my_text, multifield: raw]]");
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testMatchMappingTypeCheck() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject(); {
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                {
                    mapping.startObject("integers");
                    {
                        mapping.field("match_mapping_type", "UNKNOWN_VALUE");
                        mapping.startObject("mapping");
                        {
                            mapping.field("type", "integer");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endArray();
        }
        mapping.endObject();

        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .putMapping("test", Strings.toString(mapping))
            .settings(settings(Version.V_5_6_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Unrecognized match_mapping_type options not silently ignored",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                "breaking_60_mappings_changes.html#_unrecognized_literal_match_mapping_type_literal_options_not_silently_ignored",
            "[type: test, dynamicFieldDefinitionintegers, unknown match_mapping_type[UNKNOWN_VALUE]]");
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testBaseSimilarityDefinedCheck() {
        assertSettingsAndIssue("index.similarity.base.type", "classic",
            new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "The base similarity is now ignored as coords and query normalization have been removed." +
                    "If provided, this setting will be ignored and issue a deprecation warning",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking_60_settings_changes.html#_similarity_settings", null));
    }

    public void testIndexStoreTypeCheck() {
        assertSettingsAndIssue("index.store.type", "niofs",
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "The default index.store.type has been removed. If you were using it, " +
                    "we advise that you simply remove it from your index settings and Elasticsearch" +
                    "will use the best store implementation for your operating system.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking_60_settings_changes.html#_store_settings", null));
    }
    public void testStoreThrottleSettingsCheck() {
        assertSettingsAndIssue("index.store.throttle.max_bytes_per_sec", "32",
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "index.store.throttle settings are no longer recognized. these settings should be removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking_60_settings_changes.html#_store_throttling_settings",
                "present settings: [index.store.throttle.max_bytes_per_sec]"));
        assertSettingsAndIssue("index.store.throttle.type", "none",
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "index.store.throttle settings are no longer recognized. these settings should be removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking_60_settings_changes.html#_store_throttling_settings",
                "present settings: [index.store.throttle.type]"));
    }

    public void testSharedFileSystemSettingsCheck() {
        assertSettingsAndIssue("index.shared_filesystem", "true",
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "[index.shared_filesystem] setting should be removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/6.0/" +
                    "breaking_60_indices_changes.html#_shadow_replicas_have_been_removed", null));
    }

    public void testDelimitedPayloadFilterCheck() throws IOException {
        Settings settings = settings(
                VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, VersionUtils.getPreviousVersion(Version.V_7_0_0_alpha1)))
                .put("index.analysis.filter.my_delimited_payload_filter.type", "delimited_payload_filter")
                .put("index.analysis.filter.my_delimited_payload_filter.delimiter", "^")
                .put("index.analysis.filter.my_delimited_payload_filter.encoding", "identity").build();

        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING, "Use of 'delimited_payload_filter'.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_analysis_changes.html",
                "[The filter [my_delimited_payload_filter] is of deprecated 'delimited_payload_filter' type. "
                + "The filter type should be changed to 'delimited_payload'.]");
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }
}