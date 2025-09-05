/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SourceConfigTests extends AbstractSerializingTransformTestCase<SourceConfig> {

    private boolean lenient;

    public static SourceConfig randomSourceConfig() {
        return new SourceConfig(generateRandomStringArray(10, 10, false, false), randomQueryConfig(), randomRuntimeMappings());
    }

    public static SourceConfig randomInvalidSourceConfig() {
        // create something broken but with a source
        return new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            QueryConfigTests.randomInvalidQueryConfig(),
            randomRuntimeMappings()
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
    }

    @Override
    protected SourceConfig doParseInstance(XContentParser parser) throws IOException {
        return SourceConfig.fromXContent(parser, lenient);
    }

    @Override
    protected SourceConfig createTestInstance() {
        return lenient ? randomBoolean() ? randomSourceConfig() : randomInvalidSourceConfig() : randomSourceConfig();
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
            () -> new SourceConfig(new String[] {}, randomQueryConfig(), randomRuntimeMappings())
        );
        assertThat(e.getMessage(), is(equalTo("must specify at least one index")));
    }

    public void testConstructor_EmptyIndex() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceConfig(new String[] { "" }, randomQueryConfig(), randomRuntimeMappings())
        );
        assertThat(e.getMessage(), is(equalTo("all indices need to be non-null and non-empty")));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceConfig(new String[] { "index1", "" }, randomQueryConfig(), randomRuntimeMappings())
        );
        assertThat(e.getMessage(), is(equalTo("all indices need to be non-null and non-empty")));
    }

    public void testGetIndex() {
        SourceConfig sourceConfig = new SourceConfig(new String[] { "index1" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1")));

        sourceConfig = new SourceConfig(new String[] { "index1", "index2", "index3" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(new String[] { "index1,index2,index3" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(new String[] { "index1", "index2,index3" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "index2", "index3")));

        sourceConfig = new SourceConfig(new String[] { "index1", "remote2:index2" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "remote2:index2")));

        sourceConfig = new SourceConfig(new String[] { "index1,remote2:index2" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index1", "remote2:index2")));

        sourceConfig = new SourceConfig(new String[] { "remote1:index1", "index2" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index1", "index2")));

        sourceConfig = new SourceConfig(new String[] { "remote1:index1,index2" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index1", "index2")));

        sourceConfig = new SourceConfig(new String[] { "index*,remote2:index*" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("index*", "remote2:index*")));

        sourceConfig = new SourceConfig(new String[] { "remote1:index*,remote2:index*" }, randomQueryConfig(), randomRuntimeMappings());
        assertThat(sourceConfig.getIndex(), is(arrayContaining("remote1:index*", "remote2:index*")));
    }

    public void testGetRuntimeMappings_EmptyRuntimeMappings() {
        SourceConfig sourceConfig = new SourceConfig(generateRandomStringArray(10, 10, false, false), randomQueryConfig(), emptyMap());
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
        SourceConfig sourceConfig = new SourceConfig(generateRandomStringArray(10, 10, false, false), randomQueryConfig(), runtimeMappings);
        assertThat(sourceConfig.getRuntimeMappings(), is(equalTo(runtimeMappings)));
        assertThat(sourceConfig.getScriptBasedRuntimeMappings(), is(equalTo(scriptBasedRuntimeMappings)));
    }

    public void testRequiresRemoteCluster() {
        assertFalse(
            new SourceConfig(new String[] { "index1", "index2", "index3" }, randomQueryConfig(), randomRuntimeMappings())
                .requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(new String[] { "index1", "remote2:index2", "index3" }, randomQueryConfig(), randomRuntimeMappings())
                .requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(new String[] { "index1", "index2", "remote3:index3" }, randomQueryConfig(), randomRuntimeMappings())
                .requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(new String[] { "index1", "remote2:index2", "remote3:index3" }, randomQueryConfig(), randomRuntimeMappings())
                .requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(new String[] { "remote1:index1" }, randomQueryConfig(), randomRuntimeMappings()).requiresRemoteCluster()
        );

        assertFalse(
            new SourceConfig(new String[] { "index1,index2" }, randomQueryConfig(), randomRuntimeMappings()).requiresRemoteCluster()
        );

        assertTrue(
            new SourceConfig(new String[] { "index1,remote2:index2" }, randomQueryConfig(), randomRuntimeMappings()).requiresRemoteCluster()
        );
    }
}
