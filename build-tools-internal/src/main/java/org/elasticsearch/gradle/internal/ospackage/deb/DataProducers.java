/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage.deb;

import org.vafer.jdeb.DataConsumer;
import org.vafer.jdeb.DataProducer;
import org.vafer.jdeb.shaded.commons.compress.archivers.tar.TarArchiveEntry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Date;

/**
 * jdeb {@link DataProducer}s for a single file and a single directory entry with explicit
 * ownership and permissions.
 */
final class DataProducers {

    private DataProducers() {}

    record FileProducer(String filename, File file, String user, int uid, String group, int gid, int mode) implements DataProducer {
        @Override
        public void produce(DataConsumer receiver) throws IOException {
            try (InputStream inputStream = Files.newInputStream(file.toPath())) {
                receiver.onEachFile(inputStream, createEntry());
            }
        }

        private TarArchiveEntry createEntry() {
            TarArchiveEntry entry = new TarArchiveEntry(filename);
            entry.setUserName(user);
            entry.setUserId(uid);
            entry.setGroupName(group);
            entry.setGroupId(gid);
            entry.setMode(mode);
            entry.setSize(file.length());
            entry.setModTime(new Date(file.lastModified()));
            return entry;
        }
    }

    record DirProducer(String dirname, String user, int uid, String group, int gid, int mode) implements DataProducer {
        @Override
        public void produce(DataConsumer receiver) throws IOException {
            receiver.onEachDir(createEntry());
        }

        private TarArchiveEntry createEntry() {
            TarArchiveEntry entry = new TarArchiveEntry(dirname);
            entry.setUserName(user);
            entry.setUserId(uid);
            entry.setGroupName(group);
            entry.setGroupId(gid);
            entry.setMode(mode);
            return entry;
        }
    }
}
