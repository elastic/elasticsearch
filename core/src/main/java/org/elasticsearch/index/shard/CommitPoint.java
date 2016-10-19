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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CommitPoint {

    public static final CommitPoint NULL = new CommitPoint(-1, "_null_", Type.GENERATED, Collections.<CommitPoint.FileInfo>emptyList(), Collections.<CommitPoint.FileInfo>emptyList());

    public static class FileInfo {
        private final String name;
        private final String physicalName;
        private final long length;
        private final String checksum;

        public FileInfo(String name, String physicalName, long length, String checksum) {
            this.name = name;
            this.physicalName = physicalName;
            this.length = length;
            this.checksum = checksum;
        }

        public String name() {
            return name;
        }

        public String physicalName() {
            return this.physicalName;
        }

        public long length() {
            return length;
        }

        @Nullable
        public String checksum() {
            return checksum;
        }
    }

    public enum Type {
        GENERATED,
        SAVED
    }

    private final long version;

    private final String name;

    private final Type type;

    private final List<FileInfo> indexFiles;

    private final List<FileInfo> translogFiles;

    public CommitPoint(long version, String name, Type type, List<FileInfo> indexFiles, List<FileInfo> translogFiles) {
        this.version = version;
        this.name = name;
        this.type = type;
        this.indexFiles = Collections.unmodifiableList(new ArrayList<>(indexFiles));
        this.translogFiles = Collections.unmodifiableList(new ArrayList<>(translogFiles));
    }

    public long version() {
        return version;
    }

    public String name() {
        return this.name;
    }

    public Type type() {
        return this.type;
    }

    public List<FileInfo> indexFiles() {
        return this.indexFiles;
    }

    public List<FileInfo> translogFiles() {
        return this.translogFiles;
    }

    public boolean containPhysicalIndexFile(String physicalName) {
        return findPhysicalIndexFile(physicalName) != null;
    }

    public CommitPoint.FileInfo findPhysicalIndexFile(String physicalName) {
        for (FileInfo file : indexFiles) {
            if (file.physicalName().equals(physicalName)) {
                return file;
            }
        }
        return null;
    }

    public CommitPoint.FileInfo findNameFile(String name) {
        CommitPoint.FileInfo fileInfo = findNameIndexFile(name);
        if (fileInfo != null) {
            return fileInfo;
        }
        return findNameTranslogFile(name);
    }

    public CommitPoint.FileInfo findNameIndexFile(String name) {
        for (FileInfo file : indexFiles) {
            if (file.name().equals(name)) {
                return file;
            }
        }
        return null;
    }

    public CommitPoint.FileInfo findNameTranslogFile(String name) {
        for (FileInfo file : translogFiles) {
            if (file.name().equals(name)) {
                return file;
            }
        }
        return null;
    }
}
