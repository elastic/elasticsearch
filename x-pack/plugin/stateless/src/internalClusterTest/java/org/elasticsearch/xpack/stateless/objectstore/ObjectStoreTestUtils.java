/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;

public final class ObjectStoreTestUtils {

    private ObjectStoreTestUtils() {}

    public static MockRepository getObjectStoreMockRepository(ObjectStoreService service) {
        return getObjectStoreMockRepository(service, MockRepository.class);
    }

    public static StatelessMockRepository getObjectStoreStatelessMockRepository(ObjectStoreService service) {
        return getObjectStoreMockRepository(service, StatelessMockRepository.class);
    }

    public static <T> T getObjectStoreMockRepository(ObjectStoreService service, Class<T> repoClass) {
        var objectStore = ESTestCase.randomBoolean() ? service.getClusterObjectStore() : service.getProjectObjectStore(ProjectId.DEFAULT);
        if (repoClass.isInstance(objectStore)) {
            return repoClass.cast(objectStore);
        } else {
            throw new AssertionError("ObjectStoreService does not use a mocked BlobStoreRepository");
        }
    }

    public static BlobStoreRepository getClusterObjectStore(ObjectStoreService service) {
        return service.getClusterObjectStore();
    }

    public static BlobStoreRepository getProjectObjectStore(ObjectStoreService service, ProjectId projectId) {
        return service.getProjectObjectStore(projectId);
    }
}
