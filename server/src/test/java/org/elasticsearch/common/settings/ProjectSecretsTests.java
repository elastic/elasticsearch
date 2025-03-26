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
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.junit.Before;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ProjectSecretsTests extends AbstractNamedWriteableTestCase<ProjectSecrets> {

    private SecureClusterStateSettings secureClusterStateSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        // SecureSettings in cluster state are handled as file settings (get the byte array) both can be fetched as
        // string or file
        mockSecureSettings.setFile("foo", "bar".getBytes(StandardCharsets.UTF_8));
        mockSecureSettings.setFile("goo", "baz".getBytes(StandardCharsets.UTF_8));
        secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);
    }

    public void testGetSettings() throws Exception {
        ProjectSecrets projectSecrets = new ProjectSecrets(secureClusterStateSettings);
        assertThat(projectSecrets.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(projectSecrets.getSettings().getString("foo").toString(), equalTo("bar"));
        assertThat(new String(projectSecrets.getSettings().getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
    }

    public void testSerialize() throws Exception {
        ProjectSecrets projectSecrets = new ProjectSecrets(secureClusterStateSettings);

        final BytesStreamOutput out = new BytesStreamOutput();
        projectSecrets.writeTo(out);
        final ProjectSecrets fromStream = new ProjectSecrets(out.bytes().streamInput());

        assertThat(fromStream.getSettings().getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertEquals(projectSecrets.getSettings().getString("foo"), fromStream.getSettings().getString("foo"));
        assertThat(new String(projectSecrets.getSettings().getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
        assertTrue(fromStream.getSettings().isLoaded());
    }

    public void testToXContentChunked() {
        ProjectSecrets projectSecrets = new ProjectSecrets(secureClusterStateSettings);
        // we never serialize anything to x-content
        assertFalse(projectSecrets.toXContentChunked(EMPTY_PARAMS).hasNext());
    }

    public void testToString() {
        ProjectSecrets projectSecrets = new ProjectSecrets(secureClusterStateSettings);
        assertThat(projectSecrets.toString(), equalTo("ProjectSecrets{[all secret]}"));
    }

    @Override
    protected ProjectSecrets createTestInstance() {
        return new ProjectSecrets(
            new SecureClusterStateSettings(
                randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(15).getBytes(Charset.defaultCharset())))
            )
        );
    }

    @Override
    protected ProjectSecrets mutateInstance(ProjectSecrets instance) {
        Map<String, byte[]> updatedSettings = new HashMap<>();

        for (var settingName : instance.getSettings().getSettingNames()) {
            updatedSettings.put(settingName, instance.getSettings().getString(settingName).toString().getBytes(StandardCharsets.UTF_8));
        }
        updatedSettings.put(randomAlphaOfLength(9), randomAlphaOfLength(14).getBytes(StandardCharsets.UTF_8));
        return new ProjectSecrets(new SecureClusterStateSettings(updatedSettings));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(ProjectSecrets.class, ProjectSecrets.TYPE, ProjectSecrets::new))
        );
    }

    @Override
    protected Class<ProjectSecrets> categoryClass() {
        return ProjectSecrets.class;
    }
}
