/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Function;

public class EncryptedRepository extends BlobStoreRepository {
    static final int GCM_TAG_SIZE_IN_BYTES = 16;
    static final int GCM_IV_SIZE_IN_BYTES = 12;
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;
    static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 1 << 30;

    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final char[] masterPassword;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService
            , BlobStoreRepository delegatedRepository, char[] materPassword) {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.masterPassword = materPassword;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return null;
    }
}
