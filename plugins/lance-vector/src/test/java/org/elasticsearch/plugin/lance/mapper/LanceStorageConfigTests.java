/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.mapper;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link LanceStorageConfig}.
 */
public class LanceStorageConfigTests extends ESTestCase {

    public void testConstructorAndAccessors() {
        LanceStorageConfig config = new LanceStorageConfig("external", "file:///path/to/dataset", "_id", "vector", null, null, null);

        assertThat(config.type(), equalTo("external"));
        assertThat(config.uri(), equalTo("file:///path/to/dataset"));
        assertThat(config.idColumn(), equalTo("_id"));
        assertThat(config.vectorColumn(), equalTo("vector"));
    }

    public void testConstructorWithOssUri() {
        LanceStorageConfig config = new LanceStorageConfig(
            "external",
            "oss://my-bucket/vectors/dataset.lance",
            "doc_id",
            "embedding",
            null,
            null,
            null
        );

        assertThat(config.type(), equalTo("external"));
        assertThat(config.uri(), equalTo("oss://my-bucket/vectors/dataset.lance"));
        assertThat(config.idColumn(), equalTo("doc_id"));
        assertThat(config.vectorColumn(), equalTo("embedding"));
    }

    public void testConstructorWithEmbeddedUri() {
        LanceStorageConfig config = new LanceStorageConfig(
            "external",
            "embedded:test-vectors.json",
            "_id",
            "vector",
            null,
            null,
            null
        );

        assertThat(config.uri(), equalTo("embedded:test-vectors.json"));
    }

    public void testConstructorRequiresType() {
        expectThrows(
            NullPointerException.class,
            () -> new LanceStorageConfig(null, "file:///path", "_id", "vector", null, null, null)
        );
    }

    public void testConstructorRequiresUri() {
        expectThrows(
            NullPointerException.class,
            () -> new LanceStorageConfig("external", null, "_id", "vector", null, null, null)
        );
    }

    public void testConstructorRequiresIdColumn() {
        expectThrows(
            NullPointerException.class,
            () -> new LanceStorageConfig("external", "file:///path", null, "vector", null, null, null)
        );
    }

    public void testConstructorRequiresVectorColumn() {
        expectThrows(NullPointerException.class, () -> new LanceStorageConfig("external", "file:///path", "_id", null, null, null, null));
    }

    public void testDifferentConfigurations() {
        LanceStorageConfig config1 = new LanceStorageConfig("external", "file:///path1", "_id", "vector", null, null, null);

        LanceStorageConfig config2 = new LanceStorageConfig("local", "file:///path2", "id", "embeddings", null, null, null);

        assertThat(config1.type(), equalTo("external"));
        assertThat(config2.type(), equalTo("local"));
        assertThat(config1.uri(), equalTo("file:///path1"));
        assertThat(config2.uri(), equalTo("file:///path2"));
    }
}
