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
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {

    private final HdfsBlobStore blobStore;
    private final Path path;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore blobStore, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Boolean>() {
                @Override
                public Boolean doInHdfs(FileContext fc) throws IOException {
                    return fc.util().exists(new Path(path, blobName));
                }
            });
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Boolean>() {
            @Override
            public Boolean doInHdfs(FileContext fc) throws IOException {
                return fc.delete(new Path(path, blobName), true);
            }
        });
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Void>() {
            @Override
            public Void doInHdfs(FileContext fc) throws IOException {
                // _try_ to hsync the file before appending
                // since append is optional this is a best effort
                Path source = new Path(path, sourceBlobName);
                
                // try-with-resource is nice but since this is optional, it's hard to figure out
                // what worked and what didn't.
                // it's okay to not be able to append the file but not okay if hsync fails
                // classic try / catch to the rescue
                
                FSDataOutputStream stream = null; 
                try {
                    stream = fc.create(source, EnumSet.of(CreateFlag.APPEND, CreateFlag.SYNC_BLOCK), CreateOpts.donotCreateParent());
                } catch (IOException ex) {
                    // append is optional, ignore
                }
                if (stream != null) {
                    try (OutputStream s = stream) {
                        if (s instanceof Syncable) {
                            ((Syncable) s).hsync();
                        }
                    }
                }

                // finally rename
                fc.rename(source, new Path(path, targetBlobName));
                return null;
            }
        });
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        return SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<InputStream>() {
            @Override
            public InputStream doInHdfs(FileContext fc) throws IOException {
                return fc.open(new Path(path, blobName), blobStore.bufferSizeInBytes());
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Void>() {
            @Override
            public Void doInHdfs(FileContext fc) throws IOException {
                // don't use Streams to manually call hsync
                // note that the inputstream is NOT closed here for two reasons:
                // 1. it is closed already by ES after executing this method
                // 0. closing the stream twice causes Hadoop to issue WARNING messages which are basically noise
                // see https://issues.apache.org/jira/browse/HDFS-8099
                try (FSDataOutputStream stream = createOutput(fc, blobName)) {
                    int bytesRead;
                    byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        stream.write(buffer, 0, bytesRead);
                    }
                    stream.hsync();
                }
                return null;
            }
        });
    }

    @Override
    public void writeBlob(String blobName, BytesReference bytes) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Void>() {
            @Override
            public Void doInHdfs(FileContext fc) throws IOException {
                try (FSDataOutputStream stream = createOutput(fc, blobName)) {
                    bytes.writeTo(stream);
                    stream.hsync();
                }
                return null;
            }
        });
    }
    
    private FSDataOutputStream createOutput(FileContext fc, String blobName) throws IOException {
        return fc.create(new Path(path, blobName), EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK),
                CreateOpts.bufferSize(blobStore.bufferSizeInBytes()), CreateOpts.createParent());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(final @Nullable String blobNamePrefix) throws IOException {
        FileStatus[] files = SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileContext fc) throws IOException {
                return (!fc.util().exists(path) ? null : fc.util().listStatus(path, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return path.getName().startsWith(blobNamePrefix);
                    }
                }));
            }
        });
        if (files == null || files.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        FileStatus[] files = SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileContext fc) throws IOException {
                return (!fc.util().exists(path) ? null : fc.util().listStatus(path));
            }
        });
        if (files == null || files.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }
}