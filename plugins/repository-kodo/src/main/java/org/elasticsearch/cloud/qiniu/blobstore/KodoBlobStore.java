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

package org.elasticsearch.cloud.qiniu.blobstore;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import org.elasticsearch.cloud.qiniu.QiniuKodoClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;

/**
 *
 */
public class KodoBlobStore extends AbstractComponent implements BlobStore {

    private final QiniuKodoClient client;

    private final String bucket;

    private final String region;

    private final ByteSizeValue bufferSize;

    private final int numberOfRetries;

    private static final Logger logger = Loggers.getLogger("cloud.qiniu");


    public KodoBlobStore(Settings settings, QiniuKodoClient client, String bucket, @Nullable String region,
                         ByteSizeValue bufferSize, int maxRetries) {
        super(settings);
        this.client = client;
        this.bucket = bucket;
        this.region = region;
        this.bufferSize = bufferSize;
        this.numberOfRetries = maxRetries;

        try {
            if (!client.isBucketExist(bucket)) {
                logger.error(bucket + " is not exist. Create it before using");
            }
        } catch (QiniuException e) {
            logger.error(e);
        }

    }

    @Override
    public String toString() {
        return (region == null ? "" : region + "/") + bucket;
    }

    public QiniuKodoClient client() {
        return client;
    }

    public String bucket() {
        return bucket;
    }


    public int bufferSizeInBytes() {
        return bufferSize.bytesAsInt();
    }

    public int numberOfRetries() {
        return numberOfRetries;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new KodoBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) throws QiniuException {
        ArrayList<String> list = new ArrayList<String>();
        String marker = "";

        FileListing fileListing;
        do {
            fileListing = this.client.listObjects(this.bucket, path.buildAsString(), marker, 20);
            marker = fileListing.marker;
            FileInfo[] infos = fileListing.items;
            for (FileInfo info : infos) {
                list.add(info.key);
            }
            if (list.size() == 1000){
                this.client.deleteObjects(this.bucket, list.toArray(new String[list.size()]));
            }
            list.clear();
        } while(!fileListing.isEOF());

        this.client.deleteObjects(this.bucket, list.toArray(new String[list.size()]));
    }


    @Override
    public void close() {
    }
}
