/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.StatelessMockRepository;

import org.elasticsearch.snapshots.mockstore.MockRepository;

public final class ObjectStoreTestUtils {

    private ObjectStoreTestUtils() {}

    public static MockRepository getObjectStoreMockRepository(ObjectStoreService service) {
        return getObjectStoreMockRepository(service, MockRepository.class);
    }

    public static StatelessMockRepository getObjectStoreStatelessMockRepository(ObjectStoreService service) {
        return getObjectStoreMockRepository(service, StatelessMockRepository.class);
    }

    public static <T> T getObjectStoreMockRepository(ObjectStoreService service, Class<T> repoClass) {
        var objectStore = service.getObjectStore();
        if (repoClass.isInstance(objectStore)) {
            return repoClass.cast(objectStore);
        } else {
            throw new AssertionError("ObjectStoreService does not use a mocked BlobStoreRepository");
        }
    }
}
