/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.RemoteClusterMinimumVersionValidation;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.TestMatchers.matchesPattern;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomInvalidSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TransformConfigTests extends AbstractSerializingTransformTestCase<TransformConfig> {

    private String transformId;
    private boolean runWithHeaders;

    public static TransformConfig randomTransformConfigWithoutHeaders() {
        return randomTransformConfigWithoutHeaders(randomAlphaOfLengthBetween(1, 10));
    }

    public static TransformConfig randomTransformConfigWithoutHeaders(String id) {
        PivotConfig pivotConfig;
        LatestConfig latestConfig;
        if (randomBoolean()) {
            pivotConfig = PivotConfigTests.randomPivotConfig();
            latestConfig = null;
        } else {
            pivotConfig = null;
            latestConfig = LatestConfigTests.randomLatestConfig();
        }

        return randomTransformConfigWithoutHeaders(id, pivotConfig, latestConfig);
    }

    public static TransformConfig randomTransformConfigWithoutHeaders(String id, PivotConfig pivotConfig, LatestConfig latestConfig) {
        return new TransformConfig(
            id,
            randomSourceConfig(),
            randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            null,
            pivotConfig,
            latestConfig,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            SettingsConfigTests.randomSettingsConfig(),
            randomBoolean() ? null : randomMetadata(),
            randomBoolean() ? null : randomRetentionPolicyConfig(),
            null,
            null
        );
    }

    public static TransformConfig randomTransformConfig() {
        return randomTransformConfig(randomAlphaOfLengthBetween(1, 10));
    }

    public static TransformConfig randomTransformConfigWithDeprecatedFields(String id, Version version) {
        return randomTransformConfig(id, version, PivotConfigTests.randomPivotConfigWithDeprecatedFields(), null);
    }

    public static TransformConfig randomTransformConfig(String id) {
        return randomTransformConfig(id, randomBoolean() ? null : Version.CURRENT);
    }

    public static TransformConfig randomTransformConfig(String id, Version version) {
        PivotConfig pivotConfig;
        LatestConfig latestConfig;
        if (randomBoolean()) {
            pivotConfig = PivotConfigTests.randomPivotConfig();
            latestConfig = null;
        } else {
            pivotConfig = null;
            latestConfig = LatestConfigTests.randomLatestConfig();
        }

        return randomTransformConfig(id, version, pivotConfig, latestConfig);
    }

    public static TransformConfig randomTransformConfig(String id, Version version, PivotConfig pivotConfig, LatestConfig latestConfig) {
        return new TransformConfig(
            id,
            randomSourceConfig(),
            randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomHeaders(),
            pivotConfig,
            latestConfig,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : SettingsConfigTests.randomSettingsConfig(),
            randomBoolean() ? null : randomMetadata(),
            randomBoolean() ? null : randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            version == null ? null : version.toString()
        );
    }

    public static TransformConfig randomInvalidTransformConfig(String id) {
        if (randomBoolean()) {
            PivotConfig pivotConfig;
            LatestConfig latestConfig;
            if (randomBoolean()) {
                pivotConfig = PivotConfigTests.randomPivotConfig();
                latestConfig = null;
            } else {
                pivotConfig = null;
                latestConfig = LatestConfigTests.randomLatestConfig();
            }

            return new TransformConfig(
                id,
                randomInvalidSourceConfig(),
                randomDestConfig(),
                null,
                randomBoolean() ? randomSyncConfig() : null,
                randomHeaders(),
                pivotConfig,
                latestConfig,
                randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
                null,
                randomBoolean() ? null : randomMetadata(),
                randomBoolean() ? null : randomRetentionPolicyConfig(),
                null,
                null
            );
        } // else
        return new TransformConfig(
            id,
            randomSourceConfig(),
            randomDestConfig(),
            null,
            randomBoolean() ? randomSyncConfig() : null,
            randomHeaders(),
            PivotConfigTests.randomInvalidPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            randomBoolean() ? null : randomMetadata(),
            randomBoolean() ? null : randomRetentionPolicyConfig(),
            null,
            null
        );
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    public static RetentionPolicyConfig randomRetentionPolicyConfig() {
        return TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig();
    }

    public static Map<String, Object> randomMetadata() {
        return randomMap(0, 10, () -> {
            String key = randomAlphaOfLengthBetween(1, 10);
            Object value = switch (randomIntBetween(0, 3)) {
                case 0 -> null;
                case 1 -> randomLong();
                case 2 -> randomAlphaOfLengthBetween(1, 10);
                case 3 -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
                default -> throw new AssertionError();
            };
            return Tuple.tuple(key, value);
        });
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
        runWithHeaders = randomBoolean();
    }

    @Override
    protected TransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return TransformConfig.fromXContent(parser, transformId, runWithHeaders);
        } else {
            return TransformConfig.fromXContent(parser, null, runWithHeaders);
        }
    }

    @Override
    protected TransformConfig createTestInstance() {
        return runWithHeaders ? randomTransformConfig(transformId) : randomTransformConfigWithoutHeaders(transformId);
    }

    @Override
    protected Reader<TransformConfig> instanceReader() {
        return TransformConfig::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    private static Map<String, String> randomHeaders() {
        Map<String, String> headers = Maps.newMapWithExpectedSize(1);
        headers.put("key", "value");

        return headers;
    }

    public void testDefaultMatchAll() throws IOException {
        String pivotTransform = """
            {
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "test_match_all");
        assertNotNull(transformConfig.getSource().getQueryConfig());
        assertNotNull(transformConfig.getSource().getQueryConfig().getQuery());

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String pivotTransformWithIdAndDefaults = Strings.toString(content);

            assertThat(pivotTransformWithIdAndDefaults, matchesPattern(".*\"match_all\"\\s*:\\s*\\{\\}.*"));
        }
    }

    public void testConstructor_NoFunctionProvided() throws IOException {
        String json = """
            {
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              }
            }""";
        // Should parse with lenient parser
        createTransformConfigFromString(json, "dummy", true);
        // Should throw with strict parser
        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(json, "dummy", false));
    }

    public void testConstructor_TwoFunctionsProvided() throws IOException {
        String json = """
            {
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "latest": {
                "unique_key": [ "event1", "event2", "event3" ],
                "sort": "timestamp"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        // Should parse with lenient parser
        createTransformConfigFromString(json, "dummy", true);
        // Should throw with strict parser
        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(json, "dummy", false));
    }

    public void testPreventHeaderInjection() {
        String pivotTransform = """
            {
              "headers": {
                "key": "value"
              },
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_header_injection"));
    }

    public void testPreventCreateTimeInjection() {
        String pivotTransform = """
            {
              "create_time": %s,
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""".formatted(Instant.now().toEpochMilli());

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_createTime_injection"));
    }

    public void testPreventVersionInjection() {
        String pivotTransform = """
            {
              "version": "7.3.0",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_createTime_injection"));
    }

    public void testXContentForInternalStorage() throws IOException {
        TransformConfig transformConfig = randomTransformConfig();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transform_config\".*"));
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String doc = Strings.toString(content);

            assertFalse(doc.contains("doc_type"));
        }
    }

    public void testMaxLengthDescription() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TransformConfig(
                "id",
                randomSourceConfig(),
                randomDestConfig(),
                null,
                null,
                null,
                PivotConfigTests.randomPivotConfig(),
                null,
                randomAlphaOfLength(1001),
                null,
                null,
                null,
                null,
                null
            )
        );
        assertThat(exception.getMessage(), equalTo("[description] must be less than 1000 characters in length."));
        String description = randomAlphaOfLength(1000);
        TransformConfig config = new TransformConfig(
            "id",
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            description,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(description, equalTo(config.getDescription()));
    }

    public void testSetIdInBody() throws IOException {
        String pivotTransform = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id");
        assertEquals("body_id", transformConfig.getId());

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> createTransformConfigFromString(pivotTransform, "other_id")
        );

        assertEquals(
            "Inconsistent id; 'body_id' specified in the body differs from 'other_id' specified as a URL argument",
            ex.getCause().getMessage()
        );
    }

    public void testRewriteForUpdate() throws IOException {
        String pivotTransform = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                },
                "max_page_search_size": 111
              },
              "version": "%s"
            }""".formatted(Version.V_7_6_0.toString());

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id", true);
        TransformConfig transformConfigRewritten = TransformConfig.rewriteForUpdate(transformConfig);

        assertNull(transformConfigRewritten.getPivotConfig().getMaxPageSearchSize());
        assertNotNull(transformConfigRewritten.getSettings().getMaxPageSearchSize());
        assertEquals(111L, transformConfigRewritten.getSettings().getMaxPageSearchSize().longValue());
        assertTrue(transformConfigRewritten.getSettings().getDatesAsEpochMillis());
        assertFalse(transformConfigRewritten.getSettings().getAlignCheckpoints());

        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);
        assertEquals(Version.CURRENT, transformConfigRewritten.getVersion());
    }

    public void testRewriteForUpdateAlignCheckpoints() throws IOException {
        String pivotTransform = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              },
              "version": "%s"
            }""".formatted(Version.V_7_12_0.toString());

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id", true);
        TransformConfig transformConfigRewritten = TransformConfig.rewriteForUpdate(transformConfig);
        assertEquals(Version.CURRENT, transformConfigRewritten.getVersion());
        assertFalse(transformConfigRewritten.getSettings().getAlignCheckpoints());

        TransformConfig explicitFalseAfter715 = new TransformConfig.Builder(transformConfig).setSettings(
            new SettingsConfig.Builder(transformConfigRewritten.getSettings()).setAlignCheckpoints(false).build()
        ).setVersion(Version.V_7_15_0).build();
        transformConfigRewritten = TransformConfig.rewriteForUpdate(explicitFalseAfter715);

        assertFalse(transformConfigRewritten.getSettings().getAlignCheckpoints());
        // The config is not rewritten.
        assertEquals(Version.V_7_15_0, transformConfigRewritten.getVersion());
    }

    public void testRewriteForUpdateMaxPageSizeSearchConflicting() throws IOException {
        String pivotTransform = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                },
                "max_page_search_size": 111
              },
              "settings": {
                "max_page_search_size": 555
              },
              "version": "%s"
            }""".formatted(Version.V_7_5_0.toString());

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id", true);
        TransformConfig transformConfigRewritten = TransformConfig.rewriteForUpdate(transformConfig);

        assertNull(transformConfigRewritten.getPivotConfig().getMaxPageSearchSize());
        assertNotNull(transformConfigRewritten.getSettings().getMaxPageSearchSize());
        assertEquals(555L, transformConfigRewritten.getSettings().getMaxPageSearchSize().longValue());
        assertEquals(Version.CURRENT, transformConfigRewritten.getVersion());
        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);
    }

    public void testRewriteForBWCOfDateNormalization() throws IOException {
        String pivotTransform = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              },
              "version": "%s"
            }""".formatted(Version.V_7_6_0.toString());

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id", true);
        TransformConfig transformConfigRewritten = TransformConfig.rewriteForUpdate(transformConfig);

        assertTrue(transformConfigRewritten.getSettings().getDatesAsEpochMillis());
        assertEquals(Version.CURRENT, transformConfigRewritten.getVersion());

        TransformConfig explicitTrueAfter711 = new TransformConfig.Builder(transformConfig).setSettings(
            new SettingsConfig.Builder(transformConfigRewritten.getSettings()).setDatesAsEpochMillis(true).build()
        ).setVersion(Version.V_7_11_0).build();
        transformConfigRewritten = TransformConfig.rewriteForUpdate(explicitTrueAfter711);

        assertTrue(transformConfigRewritten.getSettings().getDatesAsEpochMillis());
        // The config is still being rewritten due to "settings.align_checkpoints".
        assertEquals(Version.CURRENT, transformConfigRewritten.getVersion());

        TransformConfig explicitTrueAfter715 = new TransformConfig.Builder(transformConfig).setSettings(
            new SettingsConfig.Builder(transformConfigRewritten.getSettings()).setDatesAsEpochMillis(true).build()
        ).setVersion(Version.V_7_15_0).build();
        transformConfigRewritten = TransformConfig.rewriteForUpdate(explicitTrueAfter715);

        assertTrue(transformConfigRewritten.getSettings().getDatesAsEpochMillis());
        // The config is not rewritten.
        assertEquals(Version.V_7_15_0, transformConfigRewritten.getVersion());
    }

    public void testGetAdditionalSourceDestValidations_WithNoRuntimeMappings() throws IOException {
        String transformWithRuntimeMappings = """
            {
              "id": "body_id",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        TransformConfig transformConfig = createTransformConfigFromString(transformWithRuntimeMappings, "body_id", true);
        assertThat(transformConfig.getAdditionalSourceDestValidations(), is(empty()));
    }

    public void testGetAdditionalSourceDestValidations_WithRuntimeMappings() throws IOException {
        String json = """
            {
              "id": "body_id",
              "source": {
                "index": "src",
                "runtime_mappings": {
                  "some-field": "some-value"
                }
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""";

        TransformConfig transformConfig = createTransformConfigFromString(json, "body_id", true);
        List<SourceDestValidation> additiionalValidations = transformConfig.getAdditionalSourceDestValidations();
        assertThat(additiionalValidations, hasSize(1));
        assertThat(additiionalValidations.get(0), is(instanceOf(RemoteClusterMinimumVersionValidation.class)));
        RemoteClusterMinimumVersionValidation remoteClusterMinimumVersionValidation =
            (RemoteClusterMinimumVersionValidation) additiionalValidations.get(0);
        assertThat(remoteClusterMinimumVersionValidation.getMinExpectedVersion(), is(equalTo(Version.V_7_12_0)));
        assertThat(remoteClusterMinimumVersionValidation.getReason(), is(equalTo("source.runtime_mappings field was set")));
    }

    public void testGroupByStayInOrder() throws IOException {
        String json = """
            {
              "id": "%s",
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "time": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "1d"
                    }
                  },
                  "alert": {
                    "terms": {
                      "field": "alert"
                    }
                  },
                  "id": {
                    "terms": {
                      "field": "id"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""".formatted(transformId);
        TransformConfig transformConfig = createTransformConfigFromString(json, transformId, true);
        List<String> originalGroups = new ArrayList<>(transformConfig.getPivotConfig().getGroupConfig().getGroups().keySet());
        assertThat(originalGroups, contains("time", "alert", "id"));
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            // Wire serialization order guarantees
            TransformConfig serialized = this.copyInstance(transformConfig);
            List<String> serializedGroups = new ArrayList<>(serialized.getPivotConfig().getGroupConfig().getGroups().keySet());
            assertThat(serializedGroups, equalTo(originalGroups));

            // Now test xcontent serialization and parsing on wire serialized object
            XContentType xContentType = randomFrom(XContentType.values()).canonical();
            BytesReference ref = XContentHelper.toXContent(serialized, xContentType, getToXContentParams(), false);
            XContentParser parser = this.createParser(XContentFactory.xContent(xContentType), ref);
            TransformConfig parsed = doParseInstance(parser);
            List<String> parsedGroups = new ArrayList<>(parsed.getPivotConfig().getGroupConfig().getGroups().keySet());
            assertThat(parsedGroups, equalTo(originalGroups));
        }
    }

    public void testCheckForDeprecations() {
        String id = randomAlphaOfLengthBetween(1, 10);
        assertThat(randomTransformConfig(id, Version.CURRENT).checkForDeprecations(xContentRegistry()), is(empty()));

        TransformConfig deprecatedConfig = randomTransformConfigWithDeprecatedFields(id, Version.CURRENT);

        // check _and_ clear warnings
        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);

        // important: checkForDeprecations does _not_ create new deprecation warnings
        assertThat(
            deprecatedConfig.checkForDeprecations(xContentRegistry()),
            equalTo(
                Collections.singletonList(
                    new DeprecationIssue(
                        Level.WARNING,
                        "Transform [" + id + "] uses the deprecated setting [max_page_search_size]",
                        TransformDeprecations.MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL,
                        TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED,
                        false,
                        null
                    )
                )
            )
        );

        deprecatedConfig = randomTransformConfigWithDeprecatedFields(id, Version.V_7_10_0);

        // check _and_ clear warnings
        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);

        // important: checkForDeprecations does _not_ create new deprecation warnings
        assertThat(
            deprecatedConfig.checkForDeprecations(xContentRegistry()),
            equalTo(
                List.of(
                    new DeprecationIssue(
                        Level.WARNING,
                        "Transform [" + id + "] uses the deprecated setting [max_page_search_size]",
                        TransformDeprecations.MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL,
                        TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED,
                        false,
                        null
                    )
                )
            )
        );

        deprecatedConfig = randomTransformConfigWithDeprecatedFields(id, Version.V_7_4_0);

        // check _and_ clear warnings
        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);

        // important: checkForDeprecations does _not_ create new deprecation warnings
        assertThat(
            deprecatedConfig.checkForDeprecations(xContentRegistry()),
            equalTo(
                List.of(
                    new DeprecationIssue(
                        Level.CRITICAL,
                        "Transform [" + id + "] uses an obsolete configuration format",
                        TransformDeprecations.UPGRADE_TRANSFORM_URL,
                        TransformDeprecations.ACTION_UPGRADE_TRANSFORMS_API,
                        false,
                        null
                    ),
                    new DeprecationIssue(
                        Level.WARNING,
                        "Transform [" + id + "] uses the deprecated setting [max_page_search_size]",
                        TransformDeprecations.MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL,
                        TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED,
                        false,
                        null
                    )
                )
            )
        );
    }

    public void testSerializingMetadataPreservesOrder() throws IOException {
        String json = """
            {
              "id": "%s",
              "_meta": {
                "d": 4,
                "a": 1,
                "c": 3,
                "e": 5,
                "b": 2
              },
              "source": {
                "index": "src"
              },
              "dest": {
                "index": "dest"
              },
              "pivot": {
                "group_by": {
                  "time": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "1d"
                    }
                  }
                },
                "aggs": {
                  "avg": {
                    "avg": {
                      "field": "points"
                    }
                  }
                }
              }
            }""".formatted(transformId);

        // Read TransformConfig from JSON and verify that metadata keys are in the same order as in JSON
        TransformConfig transformConfig = createTransformConfigFromString(json, transformId, true);
        assertThat(new ArrayList<>(transformConfig.getMetadata().keySet()), is(equalTo(List.of("d", "a", "c", "e", "b"))));

        // Write TransformConfig to JSON, read it again and verify that metadata keys are still in the same order
        json = XContentHelper.toXContent(transformConfig, XContentType.JSON, TO_XCONTENT_PARAMS, false).utf8ToString();
        transformConfig = createTransformConfigFromString(json, transformId, true);
        assertThat(new ArrayList<>(transformConfig.getMetadata().keySet()), is(equalTo(List.of("d", "a", "c", "e", "b"))));

        // Write TransformConfig to wire, read it again and verify that metadata keys are still in the same order
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            transformConfig.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                transformConfig = new TransformConfig(in);
            }
        }
        assertThat(new ArrayList<>(transformConfig.getMetadata().keySet()), is(equalTo(List.of("d", "a", "c", "e", "b"))));
    }

    private TransformConfig createTransformConfigFromString(String json, String id) throws IOException {
        return createTransformConfigFromString(json, id, false);
    }

    private TransformConfig createTransformConfigFromString(String json, String id, boolean lenient) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
        return TransformConfig.fromXContent(parser, id, lenient);
    }
}
