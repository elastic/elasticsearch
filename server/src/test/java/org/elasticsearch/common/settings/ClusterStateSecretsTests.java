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
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClusterStateSecretsTests extends AbstractNamedWriteableTestCase<ClusterSecrets> {

    private static final String SECRETS = """
        {
            "string_secrets": {
                "foo": "bar",
                "goo": "baz"
            }
        }
        """;

    private static final String OTHER_SECRETS = """
        {
            "string_secrets": {
                "foo": "other",
                "goo": "other"
            }
        }
        """;

    private SecureClusterStateSettings secureClusterStateSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        secureClusterStateSettings = parseSecrets(SECRETS);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        secureClusterStateSettings.close();
    }

    public void testGetSettings() {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        SecureSettings settings = clusterStateSecrets.getSettings();
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo").toString(), equalTo("bar"));
        assertThat(settings.getString("goo").toString(), equalTo("baz"));

        secureClusterStateSettings.close();

        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo").toString(), equalTo("bar"));
        assertThat(settings.getString("goo").toString(), equalTo("baz"));
    }

    public void testFileSettings() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        assertThat(clusterStateSecrets.getSettings().getFile("foo").readAllBytes(), equalTo("bar".getBytes(StandardCharsets.UTF_8)));
    }

    public void testSerialize() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

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
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        // we never serialize anything to x-content
        assertFalse(clusterStateSecrets.toXContentChunked(EMPTY_PARAMS).hasNext());
    }

    public void testToString() {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        assertThat(clusterStateSecrets.toString(), equalTo("ClusterStateSecrets{[all secret]}"));
    }

    public void testContainsSettings() throws IOException {
        ClusterSecrets clusterSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        assertTrue("containsSecureSettings expected to be true", clusterSecrets.containsSecureSettings(secureClusterStateSettings));
        assertTrue("containsSecureSettings expected to be true", clusterSecrets.containsSecureSettings(clusterSecrets.getSettings()));
        assertFalse("containsSecureSettings expected to be false", clusterSecrets.containsSecureSettings(parseSecrets(OTHER_SECRETS)));
    }

    public void testClose() throws Exception {
        ClusterSecrets clusterStateSecrets = new ClusterSecrets(1, secureClusterStateSettings);

        SecureSettings settings = clusterStateSecrets.getSettings();
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo").toString(), equalTo("bar"));
        assertThat(settings.getString("goo").toString(), equalTo("baz"));

        settings.close();

        // we can close the copy
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThrows(IllegalStateException.class, () -> settings.getString("foo"));
        assertThrows(IllegalStateException.class, () -> settings.getString("goo"));

        // fetching again returns a fresh object
        SecureSettings settings2 = clusterStateSecrets.getSettings();
        assertThat(settings2.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings2.getString("foo").toString(), equalTo("bar"));
        assertThat(settings2.getString("goo").toString(), equalTo("baz"));
    }

    @Override
    protected ClusterSecrets createTestInstance() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        long version = randomLong();
        int size = randomIntBetween(0, 3);
        for (int i = 0; i < size; i++) {
            secureSettings.setFile(randomAlphaOfLength(10), randomAlphaOfLength(15).getBytes(StandardCharsets.UTF_8));
        }
        return new ClusterSecrets(version, secureSettings.toSecureClusterStateSettings());
    }

    @Override
    protected ClusterSecrets mutateInstance(ClusterSecrets instance) throws IOException {
        return new ClusterSecrets(instance.getVersion() + 1L, (SecureClusterStateSettings) instance.getSettings());
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

    private static SecureClusterStateSettings parseSecrets(String secrets) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, secrets)) {
            return SecureClusterStateSettings.fromXContent(parser);
        }
    }
}
