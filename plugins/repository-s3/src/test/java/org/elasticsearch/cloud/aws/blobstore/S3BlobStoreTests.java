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

package org.elasticsearch.cloud.aws.blobstore;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class S3BlobStoreTests extends ESTestCase {
    public void testInitCannedACL() throws IOException {
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

    public void testInvalidCannedACL() throws IOException {
        try {
            S3BlobStore.initCannedACL("test_invalid");
            fail("CannedACL should fail");
        } catch (BlobStoreException ex) {
            assertThat(ex.getMessage(), equalTo("cannedACL is not valid: [test_invalid]"));
        }
    }

    public void testInitStorageClass() throws IOException {
        // it should default to `standard`
        assertThat(S3BlobStore.initStorageClass(null), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass(""), equalTo(StorageClass.Standard));

        // it should accept [standard, standard_ia, reduced_redundancy]
        assertThat(S3BlobStore.initStorageClass("standard"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("standard_ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduced_redundancy"), equalTo(StorageClass.ReducedRedundancy));
    }

    public void testCaseInsensitiveStorageClass() throws IOException {
        assertThat(S3BlobStore.initStorageClass("sTandaRd"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("sTandaRd_Ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduCED_redundancy"), equalTo(StorageClass.ReducedRedundancy));
    }

    public void testInvalidStorageClass() throws IOException {
        try {
            S3BlobStore.initStorageClass("whatever");
        } catch(BlobStoreException ex) {
            assertThat(ex.getMessage(), equalTo("`whatever` is not a valid S3 Storage Class."));
        }
    }

    public void testRejectGlacierStorageClass() throws IOException {
        try {
            S3BlobStore.initStorageClass("glacier");
        } catch(BlobStoreException ex) {
            assertThat(ex.getMessage(), equalTo("Glacier storage class is not supported"));
        }
    }
}
