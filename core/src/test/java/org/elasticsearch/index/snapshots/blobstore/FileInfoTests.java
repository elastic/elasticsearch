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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.Fields;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class FileInfoTests extends ESTestCase {

    @Test
    public void testToFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            StoreFileMetaData meta = new StoreFileMetaData("foobar", Math.abs(randomLong()), randomAsciiOfLengthBetween(1, 10), Version.LATEST, hash);
            ByteSizeValue size = new ByteSizeValue(Math.abs(randomLong()));
            BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo("_foobar", meta, size);
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            BlobStoreIndexShardSnapshot.FileInfo.toXContent(info, builder, ToXContent.EMPTY_PARAMS);
            byte[] xcontent = builder.bytes().toBytes();

            final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(xcontent)) {
                parser.nextToken();
                parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
            }
            assertThat(info.name(), equalTo(parsedInfo.name()));
            assertThat(info.physicalName(), equalTo(parsedInfo.physicalName()));
            assertThat(info.length(), equalTo(parsedInfo.length()));
            assertThat(info.checksum(), equalTo(parsedInfo.checksum()));
            assertThat(info.partBytes(), equalTo(parsedInfo.partBytes()));
            assertThat(parsedInfo.metadata().hash().length, equalTo(hash.length));
            assertThat(parsedInfo.metadata().hash(), equalTo(hash));
            assertThat(parsedInfo.metadata().writtenBy(), equalTo(Version.LATEST));
            assertThat(parsedInfo.isSame(info.metadata()), is(true));
        }
    }

    @Test
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
            long length = Math.max(0,Math.abs(randomLong()));
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
            builder.field(Fields.NAME, name);
            builder.field(Fields.PHYSICAL_NAME, physicalName);
            builder.field(Fields.LENGTH, length);
            builder.endObject();
            byte[] xContent = builder.bytes().toBytes();

            if (failure == null) {
                // No failures should read as usual
                final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
                try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(xContent)) {
                    parser.nextToken();
                    parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                }
                assertThat(name, equalTo(parsedInfo.name()));
                assertThat(physicalName, equalTo(parsedInfo.physicalName()));
                assertThat(length, equalTo(parsedInfo.length()));
                assertNull(parsedInfo.checksum());
                assertNull(parsedInfo.metadata().checksum());
                assertNull(parsedInfo.metadata().writtenBy());
            } else {
                try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(xContent)) {
                    parser.nextToken();
                    BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                    fail("Should have failed with [" + failure + "]");
                } catch (ElasticsearchParseException ex) {
                    assertThat(ex.getMessage(), containsString(failure));
                }

            }
        }
    }
}
