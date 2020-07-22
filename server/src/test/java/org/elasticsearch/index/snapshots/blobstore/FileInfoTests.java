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
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FileInfoTests extends ESTestCase {
    private static final org.apache.lucene.util.Version MIN_SUPPORTED_LUCENE_VERSION = org.elasticsearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;

    public void testToFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            StoreFileMetadata meta = new StoreFileMetadata("foobar", Math.abs(randomLong()), randomAlphaOfLengthBetween(1, 10),
                Version.LATEST, hash);
            ByteSizeValue size = new ByteSizeValue(Math.abs(randomLong()));
            BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo("_foobar", meta, size);
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            BlobStoreIndexShardSnapshot.FileInfo.toXContent(info, builder);
            byte[] xcontent = BytesReference.toBytes(BytesReference.bytes(shuffleXContent(builder)));

            final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, xcontent)) {
                parser.nextToken();
                parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
            }
            assertThat(info.name(), equalTo(parsedInfo.name()));
            assertThat(info.physicalName(), equalTo(parsedInfo.physicalName()));
            assertThat(info.length(), equalTo(parsedInfo.length()));
            assertThat(info.checksum(), equalTo(parsedInfo.checksum()));
            assertThat(info.partSize(), equalTo(parsedInfo.partSize()));
            assertThat(parsedInfo.metadata().hash().length, equalTo(hash.length));
            assertThat(parsedInfo.metadata().hash(), equalTo(hash));
            assertThat(parsedInfo.metadata().writtenBy(), equalTo(Version.LATEST));
            assertThat(parsedInfo.isSame(info.metadata()), is(true));
        }
    }

    public void testInvalidFieldsInFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            String name = "foobar";
            String physicalName = "_foobar";
            String failure = null;
            long length = Math.max(0, Math.abs(randomLong()));
            // random corruption
            switch (randomIntBetween(0, 3)) {
                case 0:
                    name = "foo,bar";
                    failure = "missing or invalid file name";
                    break;
                case 1:
                    physicalName = "_foo,bar";
                    failure = "missing or invalid physical file name";
                    break;
                case 2:
                    length = -Math.abs(randomLong());
                    failure = "missing or invalid file length";
                    break;
                case 3:
                    break;
                default:
                    fail("shouldn't be here");
            }

            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field(FileInfo.NAME, name);
            builder.field(FileInfo.PHYSICAL_NAME, physicalName);
            builder.field(FileInfo.LENGTH, length);
            builder.field(FileInfo.WRITTEN_BY, Version.LATEST.toString());
            builder.field(FileInfo.CHECKSUM, "666");
            builder.endObject();
            byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));

            if (failure == null) {
                // No failures should read as usual
                final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                }
                assertThat(name, equalTo(parsedInfo.name()));
                assertThat(physicalName, equalTo(parsedInfo.physicalName()));
                assertThat(length, equalTo(parsedInfo.length()));
                assertEquals("666", parsedInfo.checksum());
                assertEquals("666", parsedInfo.metadata().checksum());
                assertEquals(Version.LATEST, parsedInfo.metadata().writtenBy());
            } else {
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                    fail("Should have failed with [" + failure + "]");
                } catch (ElasticsearchParseException ex) {
                    assertThat(ex.getMessage(), containsString(failure));
                }
            }
        }
    }

    public void testGetPartSize() {
        BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo("foo", new StoreFileMetadata("foo", 36, "666",
            MIN_SUPPORTED_LUCENE_VERSION), new ByteSizeValue(6));
        int numBytes = 0;
        for (int i = 0; i < info.numberOfParts(); i++) {
            numBytes += info.partBytes(i);
        }
        assertEquals(numBytes, 36);

        info = new BlobStoreIndexShardSnapshot.FileInfo("foo", new StoreFileMetadata("foo", 35, "666",
            MIN_SUPPORTED_LUCENE_VERSION), new ByteSizeValue(6));
        numBytes = 0;
        for (int i = 0; i < info.numberOfParts(); i++) {
            numBytes += info.partBytes(i);
        }
        assertEquals(numBytes, 35);
        final int numIters = randomIntBetween(10, 100);
        for (int j = 0; j < numIters; j++) {
            StoreFileMetadata metadata = new StoreFileMetadata("foo", randomIntBetween(0, 1000), "666",
                MIN_SUPPORTED_LUCENE_VERSION);
            info = new BlobStoreIndexShardSnapshot.FileInfo("foo", metadata, new ByteSizeValue(randomIntBetween(1, 1000)));
            numBytes = 0;
            for (int i = 0; i < info.numberOfParts(); i++) {
                numBytes += info.partBytes(i);
            }
            assertEquals(numBytes, metadata.length());
        }

    }
}
