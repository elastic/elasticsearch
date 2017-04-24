/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class EvilNodeTests extends ESTestCase {

    public void testDefaultPathDataIncludedInPathData() throws IOException {
        final Path zero = createTempDir().toAbsolutePath();
        final Path one = createTempDir().toAbsolutePath();
        // creating hard links to directories is okay on macOS so we exercise it here
        final int random;
        if (Constants.MAC_OS_X) {
            random = randomFrom(0, 1, 2);
        } else {
            random = randomFrom(0, 1);
        }
        final Path defaultPathData;
        final Path choice = randomFrom(zero, one);
        switch (random) {
            case 0:
                defaultPathData = choice;
                break;
            case 1:
                defaultPathData = createTempDir().toAbsolutePath().resolve("link");
                Files.createSymbolicLink(defaultPathData, choice);
                break;
            case 2:
                defaultPathData = createTempDir().toAbsolutePath().resolve("link");
                Files.createLink(defaultPathData, choice);
                break;
            default:
                throw new AssertionError(Integer.toString(random));
        }
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir().toAbsolutePath())
                .put("path.data.0", zero)
                .put("path.data.1", one)
                .put("default.path.data", defaultPathData)
                .build();
        try (NodeEnvironment nodeEnv = new NodeEnvironment(settings, new Environment(settings))) {
            final Path defaultPathDataWithNodesAndId = defaultPathData.resolve("nodes/0");
            Files.createDirectories(defaultPathDataWithNodesAndId);
            final NodeEnvironment.NodePath defaultNodePath = new NodeEnvironment.NodePath(defaultPathDataWithNodesAndId);
            Files.createDirectories(defaultNodePath.indicesPath.resolve(UUIDs.randomBase64UUID()));
            final Logger mock = mock(Logger.class);
            // nothing should happen here
            Node.checkForIndexDataInDefaultPathData(settings, nodeEnv, mock);
            verifyNoMoreInteractions(mock);
        }
    }

}
