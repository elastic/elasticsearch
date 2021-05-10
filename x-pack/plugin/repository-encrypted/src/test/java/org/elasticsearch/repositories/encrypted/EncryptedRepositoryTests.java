/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.DEKS_GEN_MARKER_BLOB;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
            BlobPath.EMPTY,
            BlobPath.EMPTY.add(randomAlphaOfLength(8)),
            BlobPath.EMPTY.add(randomAlphaOfLength(4)).add(randomAlphaOfLength(4))
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
        DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("node.getId()");
        when(mockNode.getName()).thenReturn("node.getName()");
        ClusterName mockClusterName = mock(ClusterName.class);
        when(mockClusterName.value()).thenReturn("clusterName.value()");
        when(clusterService.getClusterName()).thenReturn(mockClusterName);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockMetadata.clusterUUID()).thenReturn("clusterUuid");
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(clusterService.localNode()).thenReturn(mockNode);
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
            // list blobs by prefix
            doAnswer(invocationOnMockBlobContainer -> {
                String prefix = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                Map<String, BlobMetadata> ans = new HashMap<>();
                for (BlobPath itemPath : blobsMap.keySet()) {
                    List<String> itemPathList = itemPath.parts();
                    if (itemPathList.get(itemPathList.size() - 1).startsWith(prefix)) {
                        ans.put(itemPathList.get(itemPathList.size() - 1), mock(BlobMetadata.class));
                    }
                }
                return ans;
            }).when(blobContainer).listBlobsByPrefix(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
        this.encryptedBlobStore.createPasswordGeneration(repoPassword, 0);
    }

    public void testStoreDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32); // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");

        encryptedBlobStore.storeDEK(DEKId, DEK);

        BlobPath encryptedDekPath = delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER)
            .add(EncryptedRepository.DEKS_GEN_CONTAINER)
            .add(encryptedBlobStore.inferLatestPasswordGeneration().get())
            .add(DEKId);
        BlobPath doneMarkerPath = delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER)
            .add(EncryptedRepository.DEKS_GEN_CONTAINER)
            .add(DEKS_GEN_MARKER_BLOB + encryptedBlobStore.inferLatestPasswordGeneration().get());
        assertThat(blobsMap.keySet(), containsInAnyOrder(encryptedDekPath, doneMarkerPath));
        byte[] wrappedKey = blobsMap.get(encryptedDekPath);
        SecretKey KEK = encryptedBlobStore.getKEKForDEK(repoPassword, DEKId);
        SecretKey unwrappedKey = AESKeyUtils.unwrap(KEK, wrappedKey);
        assertThat(unwrappedKey.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32); // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");
        SecretKey KEK = encryptedBlobStore.getKEKForDEK(repoPassword, DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK, DEK);
        blobsMap.put(
            delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER)
                .add(EncryptedRepository.DEKS_GEN_CONTAINER)
                .add(encryptedBlobStore.inferLatestPasswordGeneration().get())
                .add(DEKId),
            wrappedDEK
        );

        SecretKey loadedDEK = encryptedBlobStore.getDEKById(DEKId);
        assertThat(loadedDEK.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetTamperedDEKFails() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(16, 32);  // at least 128 bits because of FIPS
        SecretKey DEK = new SecretKeySpec("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8), "AES");
        SecretKey KEK = encryptedBlobStore.getKEKForDEK(repoPassword, DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK, DEK);
        int tamperPos = randomIntBetween(0, wrappedDEK.length - 1);
        wrappedDEK[tamperPos] ^= 0xFF;
        blobsMap.put(
            delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER)
                .add(EncryptedRepository.DEKS_GEN_CONTAINER)
                .add(encryptedBlobStore.inferLatestPasswordGeneration().get())
                .add(DEKId),
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
            // list blobs by prefix
            doAnswer(invocationOnMockBlobContainer -> {
                if (randomBoolean()) {
                    throw new IOException("Tested IOException");
                }
                String prefix = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                Map<String, BlobMetadata> ans = new HashMap<>();
                for (BlobPath itemPath : blobsMap.keySet()) {
                    List<String> itemPathList = itemPath.parts();
                    if (itemPathList.get(itemPathList.size() - 1).startsWith(prefix)) {
                        ans.put(itemPathList.get(itemPathList.size() - 1), mock(BlobMetadata.class));
                    }
                }
                return ans;
            }).when(blobContainer).listBlobsByPrefix(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
        IOException e = expectThrows(IOException.class, () -> encryptedBlobStore.getDEKById("this must be at least 16"));
        assertThat(e.getMessage(), containsString("Tested IOException"));
    }

    public void testGenerateKEK() {
        String id1 = "fixed identifier 1";
        String id2 = "fixed identifier 2";
        SecretKey KEK1 = encryptedBlobStore.getKEKForDEK(repoPassword, id1);
        SecretKey KEK2 = encryptedBlobStore.getKEKForDEK(repoPassword, id2);
        SecretKey sameKEK1 = encryptedBlobStore.getKEKForDEK(repoPassword, id1);
        assertThat(KEK1.getEncoded(), equalTo(sameKEK1.getEncoded()));
        assertThat(KEK1.getEncoded(), not(equalTo(KEK2.getEncoded())));
    }

}
