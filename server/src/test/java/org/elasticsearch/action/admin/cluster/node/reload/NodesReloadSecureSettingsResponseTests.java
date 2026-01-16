/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodesReloadSecureSettingsResponseTests extends ESTestCase {

    @ParametersFactory(argumentFormatting = "version=%s, exception=%s, settingNames=%s, path=%s, digest=%s, lastModifiedTime=%s")
    public static List<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();

        TransportVersion target = NodesReloadSecureSettingsResponse.NodeResponse.KEYSTORE_DETAILS;
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(target);
        TransportVersion higher = TransportVersionUtils.allReleasedVersions().higher(target);

        TransportVersion[] versions = higher != null
            ? new TransportVersion[] { target, previous, higher }
            : new TransportVersion[] { target, previous };
        Exception[] exceptions = { null, new ElasticsearchException("test error") };
        String[][] settingNamesCases = { null, {}, { "setting1", "setting2" } };
        String[] paths = { null, "/keystore" };
        String[] digests = { null, "abc123" };
        Long[] lastModifiedTimes = { null, System.currentTimeMillis() };

        for (TransportVersion version : versions) {
            for (Exception exception : exceptions) {
                for (String[] settingNames : settingNamesCases) {
                    for (String path : paths) {
                        for (String digest : digests) {
                            for (Long lastModifiedTime : lastModifiedTimes) {
                                parameters.add(new Object[] { version, exception, settingNames, path, digest, lastModifiedTime });
                            }
                        }
                    }
                }
            }
        }

        return parameters;
    }

    private final TransportVersion version;
    private final Exception exception;
    private final String[] settingNames;
    private final String path;
    private final String digest;
    private final Long lastModifiedTime;

    public NodesReloadSecureSettingsResponseTests(
        TransportVersion version,
        Exception exception,
        String[] settingNames,
        String path,
        String digest,
        Long lastModifiedTime
    ) {
        this.version = version;
        this.exception = exception;
        this.settingNames = settingNames;
        this.path = path;
        this.digest = digest;
        this.lastModifiedTime = lastModifiedTime;
    }

    public void testNodeResponseWriteAndRead() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-id");
        var nr = new NodesReloadSecureSettingsResponse.NodeResponse(node, exception, settingNames, path, digest, lastModifiedTime);
        verifyWriteAndRead(nr);
    }

    private void verifyWriteAndRead(NodesReloadSecureSettingsResponse.NodeResponse nr) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(version);
        nr.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(version);
        var readNr = new NodesReloadSecureSettingsResponse.NodeResponse(in);

        assertEquals(nr.getNode(), readNr.getNode());
        assertEquals(nr.reloadException() == null, readNr.reloadException() == null);
        if (nr.reloadException() != null) {
            assertNotNull(readNr.reloadException());
            assertEquals(nr.reloadException().getMessage(), readNr.reloadException().getMessage());
        }
        if (version.supports(NodesReloadSecureSettingsResponse.NodeResponse.KEYSTORE_DETAILS)) {
            assertArrayEquals(nr.secureSettingNames(), readNr.secureSettingNames());
            assertEquals(nr.keystorePath(), readNr.keystorePath());
            assertEquals(nr.keystoreDigest(), readNr.keystoreDigest());
            assertEquals(nr.keystoreLastModifiedTime(), readNr.keystoreLastModifiedTime());
        } else {
            assertNull(readNr.secureSettingNames());
            assertNull(readNr.keystorePath());
            assertNull(readNr.keystoreDigest());
            assertNull(readNr.keystoreLastModifiedTime());
        }
    }
}
