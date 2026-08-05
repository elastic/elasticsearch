/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.core.transform.transforms.QueryConfigTests.randomQueryConfig;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SourceConfigTests extends AbstractSerializingTransformTestCase<SourceConfig> {

    private boolean lenient;
    private boolean crossProject;

    public static SourceConfig randomSourceConfig() {
        return randomSourceConfig(false);
    }

    private static SourceConfig randomSourceConfig(boolean crossProject) {
        return new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            randomQueryConfig(),
            randomRuntimeMappings(),
            crossProject ? IndicesOptions.CPS_LENIENT_EXPAND_OPEN : IndicesOptions.LENIENT_EXPAND_OPEN,
            randomStringOrNull()
        );
    }

    private IndicesOptions indicesOptions() {
        return crossProject ? IndicesOptions.CPS_LENIENT_EXPAND_OPEN : IndicesOptions.LENIENT_EXPAND_OPEN;
    }

    private static String randomStringOrNull() {
        return randomBoolean() ? randomAlphanumericOfLength(5) : null;
    }

    public static SourceConfig randomInvalidSourceConfig() {
        return randomInvalidSourceConfig(false);
    }

    private static SourceConfig randomInvalidSourceConfig(boolean crossProject) {
        // create something broken but with a source
        return new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            QueryConfigTests.randomInvalidQueryConfig(),
            randomRuntimeMappings(),
            crossProject ? IndicesOptions.CPS_LENIENT_EXPAND_OPEN : IndicesOptions.LENIENT_EXPAND_OPEN,
            randomStringOrNull()
        );
    }

    private static Map<String, Object> randomRuntimeMappings() {
        return randomList(0, 10, () -> randomAlphaOfLengthBetween(1, 10)).stream()
            .distinct()
            .collect(toMap(f -> f, f -> singletonMap("type", randomFrom("boolean", "date", "double", "keyword", "long"))));
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
        crossProject = randomBoolean();
    }

    @Override
    protected SourceConfig doParseInstance(XContentParser parser) throws IOException {
        return SourceConfig.fromXContent(parser, lenient, new TransformParsingContext(crossProject));
    }

    @Override
    protected SourceConfig createTestInstance() {
        return lenient
            ? randomBoolean() ? randomSourceConfig(crossProject) : randomInvalidSourceConfig(crossProject)
            : randomSourceConfig(crossProject);
    }

    @Override
    protected SourceConfig mutateInstance(SourceConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only as QueryConfig stores a Map<String, Object>
        return field -> field.isEmpty() == false;
    }

    @Override
    protected Reader<SourceConfig> instanceReader() {
        return SourceConfig::new;
    }

    public void testConstructor_NoIndices() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceConfig(new String[] {}, randomQueryConfig(), randomRuntimeMappings(), indicesOptions(), randomStringOrNull())
        );
        assertThat(e.getMessage(), is(equalTo("must specify at least one index")));
    }

    public void testConstructor_EmptyIndex() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceConfig(
                new String[] { "" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            )
        );
        assertThat(e.getMessage(), is(equalTo("all indices need to be non-null and non-empty")));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceConfig(
                new String[] { "index1", "" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            )
        );
        assertThat(e.getMessage(), is(equalTo("all indices need to be non-null and non-empty")));
    }

    public void testGetIndex() {
        SourceConfig sourceConfig = new SourceConfig(
            new String[] { "index1" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1")));

        sourceConfig = new SourceConfig(
            new String[] { "index1", "index2", "index3" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(
            new String[] { "index1,index2,index3" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(
            new String[] { "index1", "index2,index3" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(
            new String[] { "index1", "remote2:index2" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "remote2:index2")));

        sourceConfig = new SourceConfig(
            new String[] { "index1,remote2:index2" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "remote2:index2")));

        sourceConfig = new SourceConfig(
            new String[] { "remote1:index1", "index2" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index1", "index2")));

        sourceConfig = new SourceConfig(
            new String[] { "remote1:index1,index2" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index1", "index2")));

        sourceConfig = new SourceConfig(
            new String[] { "index*,remote2:index*" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index*", "remote2:index*")));

        sourceConfig = new SourceConfig(
            new String[] { "remote1:index*,remote2:index*" },
            randomQueryConfig(),
            randomRuntimeMappings(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index*", "remote2:index*")));
    }

    public void testGetRuntimeMappings_EmptyRuntimeMappings() {
        SourceConfig sourceConfig = new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            randomQueryConfig(),
            emptyMap(),
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getRuntimeMappings(), is(anEmptyMap()));
        assertThat(sourceConfig.getScriptBasedRuntimeMappings(), is(anEmptyMap()));
    }

    public void testGetRuntimeMappings_NonEmptyRuntimeMappings() {
        Map<String, Object> runtimeMappings = Map.of(
            "field-A",
            Map.of("type", "keyword"),
            "field-B",
            Map.of("script", "some script"),
            "field-C",
            Map.of("script", "some other script")
        );
        Map<String, Object> scriptBasedRuntimeMappings = Map.of(
            "field-B",
            Map.of("script", "some script"),
            "field-C",
            Map.of("script", "some other script")
        );
        SourceConfig sourceConfig = new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            randomQueryConfig(),
            runtimeMappings,
            indicesOptions(),
            randomStringOrNull()
        );
        assertThat(sourceConfig.getRuntimeMappings(), is(equalTo(runtimeMappings)));
        assertThat(sourceConfig.getScriptBasedRuntimeMappings(), is(equalTo(scriptBasedRuntimeMappings)));
    }

    public void testWithProjectRouting() {
        SourceConfig original = new SourceConfig(
            new String[] { "index1", "index2" },
            randomQueryConfig(),
            Map.of("field", Map.of("type", "keyword")),
            indicesOptions(),
            null
        );

        SourceConfig withRouting = original.withProjectRouting("_alias:_origin");
        assertThat(withRouting.getProjectRouting(), equalTo("_alias:_origin"));
        assertThat(withRouting.getIndex(), equalTo(original.getIndex()));
        assertThat(withRouting.getQueryConfig(), equalTo(original.getQueryConfig()));
        assertThat(withRouting.getRuntimeMappings(), equalTo(original.getRuntimeMappings()));
        assertThat(withRouting.indicesOptions(), equalTo(original.indicesOptions()));

        SourceConfig cleared = withRouting.withProjectRouting(null);
        assertThat(cleared.getProjectRouting(), is(equalTo(null)));
        assertThat(cleared.getIndex(), equalTo(original.getIndex()));
    }

    /**
     * {@code project_routing} is declared via {@code declareString}, which only accepts
     * {@code VALUE_STRING} tokens — not {@code VALUE_NULL} — for both the strict and lenient
     * parsers (the declaration doesn't branch on {@code lenient}). So a caller cannot express
     * "clear project_routing" as a literal JSON {@code null}; the request must omit the field
     * entirely instead. This matters for {@code _update}: `TransformUpdater` treats "the update
     * explicitly supplied a source config" as an opt-out from the migration LOCAL_ONLY default —
     * but only when the source config is parseable in the first place.
     */
    public void testProjectRoutingRejectsExplicitNull() throws IOException {
        String source = """
            {
              "index": ["index1"],
              "project_routing": null
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> SourceConfig.fromXContent(parser, false, new TransformParsingContext(crossProject))
            );
            assertThat(e.getMessage(), containsString("project_routing"));
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> SourceConfig.fromXContent(parser, true, new TransformParsingContext(crossProject))
            );
            assertThat(e.getMessage(), containsString("project_routing"));
        }
    }

    public void testRequiresRemoteCluster() {
        assertFalse(
            new SourceConfig(
                new String[] { "index1", "index2", "index3" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(
                new String[] { "index1", "remote2:index2", "index3" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(
                new String[] { "index1", "index2", "remote3:index3" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(
                new String[] { "index1", "remote2:index2", "remote3:index3" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(
                new String[] { "remote1:index1" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertFalse(
            new SourceConfig(
                new String[] { "index1,index2" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(
                new String[] { "index1,remote2:index2" },
                randomQueryConfig(),
                randomRuntimeMappings(),
                indicesOptions(),
                randomStringOrNull()
            ).requiresRemoteCluster()
        );
    }

    @Override
    protected SourceConfig mutateInstanceForVersion(SourceConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static SourceConfig mutateForVersion(SourceConfig instance, TransportVersion version) {
        return new SourceConfig(
            instance.getIndex(),
            instance.getQueryConfig(),
            instance.getRuntimeMappings(),
            version.supports(SourceConfig.TRANSFORM_INDICES_OPTIONS) ? instance.indicesOptions() : IndicesOptions.LENIENT_EXPAND_OPEN,
            version.supports(SourceConfig.TRANSFORM_PROJECT_ROUTING) ? instance.getProjectRouting() : null
        );
    }
}
