/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptedRepositoryTests extends ESTestCase {

    private SecureString repoPassword;
    private BlobPath delegatedPath;
    private BlobStore delegatedBlobStore;
    private BlobStoreRepository delegatedRepository;
    private RepositoryMetadata repositoryMetadata;
    private EncryptedRepository encryptedRepository;
    private EncryptedRepository.EncryptedBlobStore encryptedBlobStore;
    private Map<BlobPath, byte[]> blobsMap;

    @Before
    public void setUpMocks() throws Exception {
        this.repoPassword = new SecureString(randomAlphaOfLength(20).toCharArray());
        this.delegatedPath = randomFrom(
            BlobPath.cleanPath(),
            BlobPath.cleanPath().add(randomAlphaOfLength(8)),
            BlobPath.cleanPath().add(randomAlphaOfLength(4)).add(randomAlphaOfLength(4))
        );
        this.delegatedBlobStore = mock(BlobStore.class);
        this.delegatedRepository = mock(BlobStoreRepository.class);
        when(delegatedRepository.blobStore()).thenReturn(delegatedBlobStore);
        when(delegatedRepository.basePath()).thenReturn(delegatedPath);
        this.repositoryMetadata = new RepositoryMetadata(
            randomAlphaOfLength(4),
            EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME,
            Settings.EMPTY
        );
        ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(mock(ThreadPool.class));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        this.encryptedRepository = new EncryptedRepository(
            repositoryMetadata,
            mock(NamedXContentRegistry.class),
            clusterService,
            mock(BigArrays.class),
            mock(RecoverySettings.class),
            delegatedRepository,
            () -> mock(XPackLicenseState.class),
            repoPassword
        );
        this.encryptedBlobStore = (EncryptedRepository.EncryptedBlobStore) encryptedRepository.createBlobStore();
        this.blobsMap = new HashMap<>();
        doAnswer(invocationOnMockBlobStore -> {
            BlobPath blobPath = ((BlobPath) invocationOnMockBlobStore.getArguments()[0]);
            BlobContainer blobContainer = mock(BlobContainer.class);
            // write atomic
            doAnswer(invocationOnMockBlobContainer -> {
                String DEKId = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                BytesReference DEKBytesReference = ((BytesReference) invocationOnMockBlobContainer.getArguments()[1]);
                this.blobsMap.put(blobPath.add(DEKId), BytesReference.toBytes(DEKBytesReference));
                return null;
            }).when(blobContainer).writeBlobAtomic(any(String.class), any(BytesReference.class), anyBoolean());
            // read
            doAnswer(invocationOnMockBlobContainer -> {
                String DEKId = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                return new ByteArrayInputStream(blobsMap.get(blobPath.add(DEKId)));
            }).when(blobContainer).readBlob(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
    }

    public void testStoreDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32); // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");

        encryptedBlobStore.storeDEK(DEKId, DEK);

        SecretKey KEK = encryptedRepository.generateKEK(DEKId);
        assertThat(
            blobsMap.keySet(),
            contains(
                delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(EncryptedRepository.DEKS_GEN_CONTAINER).add("0").add(DEKId)
            )
        );
        byte[] wrappedKey = blobsMap.values().iterator().next();
        SecretKey unwrappedKey = AESKeyUtils.unwrap(KEK, wrappedKey);
        assertThat(unwrappedKey.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32); // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");
        SecretKey KEK = encryptedRepository.generateKEK(DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK, DEK);
        blobsMap.put(
            delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(EncryptedRepository.DEKS_GEN_CONTAINER).add("0").add(DEKId),
            wrappedDEK
        );

        SecretKey loadedDEK = encryptedBlobStore.getDEKById(DEKId);
        assertThat(loadedDEK.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetTamperedDEKFails() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32);  // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8), "AES");
        SecretKey KEK = encryptedRepository.generateKEK(DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK, DEK);
        int tamperPos = randomIntBetween(0, wrappedDEK.length - 1);
        wrappedDEK[tamperPos] ^= 0xFF;
        blobsMap.put(
            delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(EncryptedRepository.DEKS_GEN_CONTAINER).add("0").add(DEKId),
            wrappedDEK
        );

        RepositoryException e = expectThrows(RepositoryException.class, () -> encryptedBlobStore.getDEKById(DEKId));
        assertThat(e.repository(), equalTo(repositoryMetadata.name()));
        assertThat(e.getMessage(), containsString("Failure to AES unwrap the DEK"));
    }

    public void testGetDEKIOException() {
        doAnswer(invocationOnMockBlobStore -> {
            BlobPath blobPath = ((BlobPath) invocationOnMockBlobStore.getArguments()[0]);
            BlobContainer blobContainer = mock(BlobContainer.class);
            // read
            doAnswer(invocationOnMockBlobContainer -> { throw new IOException("Tested IOException"); }).when(blobContainer)
                .readBlob(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
        IOException e = expectThrows(IOException.class, () -> encryptedBlobStore.getDEKById("this must be at least 16"));
        assertThat(e.getMessage(), containsString("Tested IOException"));
    }

    public void testGenerateKEK() {
        String id1 = "fixed identifier 1";
        String id2 = "fixed identifier 2";
        SecretKey KEK1 = encryptedRepository.generateKEK(id1);
        SecretKey KEK2 = encryptedRepository.generateKEK(id2);
        SecretKey sameKEK1 = encryptedRepository.generateKEK(id1);
        assertThat(KEK1.getEncoded(), equalTo(sameKEK1.getEncoded()));
        assertThat(KEK1.getEncoded(), not(equalTo(KEK2.getEncoded())));
    }

}
