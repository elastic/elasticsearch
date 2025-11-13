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
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class NodesReloadSecureSettingsResponseTests extends ESTestCase {

    @ParametersFactory(argumentFormatting = "version=%s, exception=%s, settingNames=%s, lastModifiedTime=%s")
    public static List<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();

        TransportVersion current = TransportVersion.current();
        List<TransportVersion> versions = List.of(current, TransportVersionUtils.getPreviousVersion(current));

        List<Optional<Exception>> exceptions = List.of(Optional.empty(), Optional.of(new ElasticsearchException("test error")));

        List<Optional<Collection<String>>> settingNamesCases = List.of(
            Optional.empty(),
            Optional.of(List.of()),
            Optional.of(List.of("setting1", "setting2"))
        );

        List<Optional<Long>> lastModifiedTimes = List.of(Optional.empty(), Optional.of(System.currentTimeMillis()));

        for (TransportVersion version : versions) {
            for (Optional<Exception> exception : exceptions) {
                for (Optional<Collection<String>> settingNames : settingNamesCases) {
                    for (Optional<Long> lastModifiedTime : lastModifiedTimes) {
                        parameters.add(
                            new Object[] { version, exception.orElse(null), settingNames.orElse(null), lastModifiedTime.orElse(null) }
                        );
                    }
                }
            }
        }

        return parameters;
    }

    private final TransportVersion version;
    private final Exception exception;
    private final Collection<String> settingNames;
    private final Long lastModifiedTime;

    public NodesReloadSecureSettingsResponseTests(
        TransportVersion version,
        Exception exception,
        Collection<String> settingNames,
        Long lastModifiedTime
    ) {
        this.version = version;
        this.exception = exception;
        this.settingNames = settingNames;
        this.lastModifiedTime = lastModifiedTime;
    }

    public void testNodeResponseWriteAndRead() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-id");
        var nr = new NodesReloadSecureSettingsResponse.NodeResponse(node, exception, settingNames, lastModifiedTime);
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
            assertEquals(nr.reloadException().getMessage(), readNr.reloadException().getMessage());
        }
        if (version.equals(TransportVersion.current())) {
            assertEquals(nr.secureSettingNames(), readNr.secureSettingNames());
            assertEquals(nr.keystoreLastModifiedTime(), readNr.keystoreLastModifiedTime());
        } else {
            assertNull(readNr.secureSettingNames());
            assertNull(readNr.keystoreLastModifiedTime());
        }
    }
}
