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

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ESBlobStoreTestCase;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.equalTo;

public class S3BlobStoreTests extends ESBlobStoreTestCase {

    @Override
    protected BlobStore newBlobStore() {
        return randomMockS3BlobStore();
    }

    public void testInitCannedACL() {
        String[] aclList = new String[]{
                "private", "public-read", "public-read-write", "authenticated-read",
                "log-delivery-write", "bucket-owner-read", "bucket-owner-full-control"};

        //empty acl
        assertThat(S3BlobStore.initCannedACL(null), equalTo(CannedAccessControlList.Private));
        assertThat(S3BlobStore.initCannedACL(""), equalTo(CannedAccessControlList.Private));

        // it should init cannedACL correctly
        for (String aclString : aclList) {
            CannedAccessControlList acl = S3BlobStore.initCannedACL(aclString);
            assertThat(acl.toString(), equalTo(aclString));
        }

        // it should accept all aws cannedACLs
        for (CannedAccessControlList awsList : CannedAccessControlList.values()) {
            CannedAccessControlList acl = S3BlobStore.initCannedACL(awsList.toString());
            assertThat(acl, equalTo(awsList));
        }
    }

    public void testInvalidCannedACL() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initCannedACL("test_invalid"));
        assertThat(ex.getMessage(), equalTo("cannedACL is not valid: [test_invalid]"));
    }

    public void testInitStorageClass() {
        // it should default to `standard`
        assertThat(S3BlobStore.initStorageClass(null), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass(""), equalTo(StorageClass.Standard));

        // it should accept [standard, standard_ia, reduced_redundancy, intelligent_tiering]
        assertThat(S3BlobStore.initStorageClass("standard"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("standard_ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduced_redundancy"), equalTo(StorageClass.ReducedRedundancy));
        assertThat(S3BlobStore.initStorageClass("intelligent_tiering"), equalTo(StorageClass.IntelligentTiering));
    }

    public void testCaseInsensitiveStorageClass() {
        assertThat(S3BlobStore.initStorageClass("sTandaRd"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("sTandaRd_Ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduCED_redundancy"), equalTo(StorageClass.ReducedRedundancy));
        assertThat(S3BlobStore.initStorageClass("intelLigeNt_tieriNG"), equalTo(StorageClass.IntelligentTiering));
    }

    public void testInvalidStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("whatever"));
        assertThat(ex.getMessage(), equalTo("`whatever` is not a valid S3 Storage Class."));
    }

    public void testRejectGlacierStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("glacier"));
        assertThat(ex.getMessage(), equalTo("Glacier storage class is not supported"));
    }

    /**
     * Creates a new {@link S3BlobStore} with random settings.
     * <p>
     * The blobstore uses a {@link MockAmazonS3} client.
     */
    public static S3BlobStore randomMockS3BlobStore() {
        String bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        ByteSizeValue bufferSize = new ByteSizeValue(randomIntBetween(5, 100), ByteSizeUnit.MB);
        boolean serverSideEncryption = randomBoolean();

        String cannedACL = null;
        if (randomBoolean()) {
            cannedACL = randomFrom(CannedAccessControlList.values()).toString();
        }

        String storageClass = null;
        if (randomBoolean()) {
            storageClass = randomValueOtherThan(StorageClass.Glacier, () -> randomFrom(StorageClass.values())).toString();
        }

        final AmazonS3 client = new MockAmazonS3(new ConcurrentHashMap<>(), bucket, serverSideEncryption, cannedACL, storageClass);
        final S3Service service = new S3Service() {
            @Override
            public synchronized AmazonS3Reference client(RepositoryMetaData repositoryMetaData) {
                return new AmazonS3Reference(client);
            }
        };
        return new S3BlobStore(service, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass,
            new RepositoryMetaData(bucket, "s3", Settings.EMPTY));
    }
}
