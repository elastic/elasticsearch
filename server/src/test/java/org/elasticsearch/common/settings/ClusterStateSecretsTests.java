/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateSecretsTests extends AbstractNamedWriteableTestCase<ClusterSecrets> {

    private static final String MOUNTED_SETTINGS = """
        {
            "metadata": {
                "version": "1",
                "compatibility": "8.7.0"
            },
            "secrets": {
                "foo": "bar",
                "goo": "baz"
            }
        }
        """;

    private LocallyMountedSecrets locallyMountedSecrets;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Environment environment = newEnvironment();
        writeTestFile(environment.configDir().resolve("secrets").resolve("secrets.json"), MOUNTED_SETTINGS);
        locallyMountedSecrets = new LocallyMountedSecrets(environment);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        locallyMountedSecrets.close();
    }

    public void testGetSettings() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        assertThat(clusterStateSecrets.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(clusterStateSecrets.getSettings().getString("foo").toString(), equalTo("bar"));
        assertThat(clusterStateSecrets.getSettings().getString("goo").toString(), equalTo("baz"));

        locallyMountedSecrets.close();

        assertThat(clusterStateSecrets.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(clusterStateSecrets.getSettings().getString("foo").toString(), equalTo("bar"));
        assertThat(clusterStateSecrets.getSettings().getString("goo").toString(), equalTo("baz"));
    }

    public void testFileSettings() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        assertThat(clusterStateSecrets.getSettings().getFile("foo").readAllBytes(), equalTo("bar".getBytes(StandardCharsets.UTF_8)));
    }

    public void testSerialize() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        final BytesStreamOutput out = new BytesStreamOutput();
        clusterStateSecrets.writeTo(out);
        final ClusterSecrets fromStream = new ClusterSecrets(out.bytes().streamInput());

        assertThat(fromStream.getVersion(), equalTo(clusterStateSecrets.getVersion()));

        assertThat(fromStream.getSettings().getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertEquals(clusterStateSecrets.getSettings().getString("foo"), fromStream.getSettings().getString("foo"));
        assertTrue(fromStream.getSettings().isLoaded());
    }

    public void testToXContentChunked() {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        // we never serialize anything to x-content
        assertFalse(clusterStateSecrets.toXContentChunked(EMPTY_PARAMS).hasNext());
    }

    public void testToString() {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        assertThat(clusterStateSecrets.toString(), equalTo("ClusterStateSecrets{[all secret]}"));
    }

    public void testClose() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, new SecureClusterStateSettings(locallyMountedSecrets));

        SecureSettings settings = clusterStateSecrets.getSettings();
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo").toString(), equalTo("bar"));
        assertThat(settings.getString("goo").toString(), equalTo("baz"));

        settings.close();

        // we can close the copy
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo"), nullValue());
        assertThat(settings.getString("goo"), nullValue());

        // fetching again returns a fresh object
        SecureSettings settings2 = clusterStateSecrets.getSettings();
        assertThat(settings2.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings2.getString("foo").toString(), equalTo("bar"));
        assertThat(settings2.getString("goo").toString(), equalTo("baz"));
    }

    @Override
    protected ClusterSecrets createTestInstance() {
        return new ClusterSecrets(
            randomLong(),
            new SecureClusterStateSettings(
                randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(15).getBytes(Charset.defaultCharset())))
            )
        );
    }

    @Override
    protected ClusterSecrets mutateInstance(ClusterSecrets instance) throws IOException {
        return new ClusterSecrets(instance.getVersion() + 1L, new SecureClusterStateSettings(instance.getSettings()));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(ClusterSecrets.class, ClusterSecrets.TYPE, ClusterSecrets::new))
        );
    }

    @Override
    protected Class<ClusterSecrets> categoryClass() {
        return ClusterSecrets.class;
    }

    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, contents);
        Files.createDirectories(path.getParent());
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
