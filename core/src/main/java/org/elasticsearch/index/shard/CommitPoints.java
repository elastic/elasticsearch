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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CommitPoints implements Iterable<CommitPoint> {

    private final List<CommitPoint> commitPoints;

    public CommitPoints(List<CommitPoint> commitPoints) {
        CollectionUtil.introSort(commitPoints, new Comparator<CommitPoint>() {
            @Override
            public int compare(CommitPoint o1, CommitPoint o2) {
                return (o2.version() < o1.version() ? -1 : (o2.version() == o1.version() ? 0 : 1));
            }
        });
        this.commitPoints = Collections.unmodifiableList(new ArrayList<>(commitPoints));
    }

    public List<CommitPoint> commits() {
        return this.commitPoints;
    }

    public boolean hasVersion(long version) {
        for (CommitPoint commitPoint : commitPoints) {
            if (commitPoint.version() == version) {
                return true;
            }
        }
        return false;
    }

    public CommitPoint.FileInfo findPhysicalIndexFile(String physicalName) {
        for (CommitPoint commitPoint : commitPoints) {
            CommitPoint.FileInfo fileInfo = commitPoint.findPhysicalIndexFile(physicalName);
            if (fileInfo != null) {
                return fileInfo;
            }
        }
        return null;
    }

    public CommitPoint.FileInfo findNameFile(String name) {
        for (CommitPoint commitPoint : commitPoints) {
            CommitPoint.FileInfo fileInfo = commitPoint.findNameFile(name);
            if (fileInfo != null) {
                return fileInfo;
            }
        }
        return null;
    }

    @Override
    public Iterator<CommitPoint> iterator() {
        return commitPoints.iterator();
    }

    public static byte[] toXContent(CommitPoint commitPoint) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject();
        builder.field("version", commitPoint.version());
        builder.field("name", commitPoint.name());
        builder.field("type", commitPoint.type().toString());

        builder.startObject("index_files");
        for (CommitPoint.FileInfo fileInfo : commitPoint.indexFiles()) {
            builder.startObject(fileInfo.name());
            builder.field("physical_name", fileInfo.physicalName());
            builder.field("length", fileInfo.length());
            if (fileInfo.checksum() != null) {
                builder.field("checksum", fileInfo.checksum());
            }
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("translog_files");
        for (CommitPoint.FileInfo fileInfo : commitPoint.translogFiles()) {
            builder.startObject(fileInfo.name());
            builder.field("physical_name", fileInfo.physicalName());
            builder.field("length", fileInfo.length());
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();
        return BytesReference.toBytes(builder.bytes());
    }

    public static CommitPoint fromXContent(byte[] data) throws Exception {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY, data)) {
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                throw new IOException("No commit point data");
            }
            long version = -1;
            String name = null;
            CommitPoint.Type type = null;
            List<CommitPoint.FileInfo> indexFiles = new ArrayList<>();
            List<CommitPoint.FileInfo> translogFiles = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    List<CommitPoint.FileInfo> files = null;
                    if ("index_files".equals(currentFieldName) || "indexFiles".equals(currentFieldName)) {
                        files = indexFiles;
                    } else if ("translog_files".equals(currentFieldName) || "translogFiles".equals(currentFieldName)) {
                        files = translogFiles;
                    } else {
                        throw new IOException("Can't handle object with name [" + currentFieldName + "]");
                    }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            String fileName = currentFieldName;
                            String physicalName = null;
                            long size = -1;
                            String checksum = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("physical_name".equals(currentFieldName) || "physicalName".equals(currentFieldName)) {
                                        physicalName = parser.text();
                                    } else if ("length".equals(currentFieldName)) {
                                        size = parser.longValue();
                                    } else if ("checksum".equals(currentFieldName)) {
                                        checksum = parser.text();
                                    }
                                }
                            }
                            if (physicalName == null) {
                                throw new IOException("Malformed commit, missing physical_name for [" + fileName + "]");
                            }
                            if (size == -1) {
                                throw new IOException("Malformed commit, missing length for [" + fileName + "]");
                            }
                            files.add(new CommitPoint.FileInfo(fileName, physicalName, size, checksum));
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        version = parser.longValue();
                    } else if ("name".equals(currentFieldName)) {
                        name = parser.text();
                    } else if ("type".equals(currentFieldName)) {
                        type = CommitPoint.Type.valueOf(parser.text());
                    }
                }
            }

            if (version == -1) {
                throw new IOException("Malformed commit, missing version");
            }
            if (name == null) {
                throw new IOException("Malformed commit, missing name");
            }
            if (type == null) {
                throw new IOException("Malformed commit, missing type");
            }

            return new CommitPoint(version, name, type, indexFiles, translogFiles);
        }
    }
}
