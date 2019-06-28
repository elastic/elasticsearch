package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESTestCase.assertBusy;

public class S3BlobStoreTestUtil extends BlobStoreTestUtil {
    public S3BlobStoreTestUtil(BlobStoreRepository repository) {
        super(repository);
    }

    @Override
    public void assertCorruptionVisible(Map<String, Set<String>> indexToFiles) throws Exception {
        assertBusy(() -> super.assertCorruptionVisible(indexToFiles), 10L, TimeUnit.MINUTES);
    }

    @Override
    public void assertConsistency() throws Exception {
        assertBusy(() -> super.assertConsistency(), 10L, TimeUnit.MINUTES);
    }

    @Override
    public void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetaData> blobs) throws Exception {
        assertBusy(() -> super.assertBlobsByPrefix(path, prefix, blobs), 10L, TimeUnit.MINUTES);
    }

    @Override
    public void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        assertBusy(() -> super.assertChildren(path, children), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertDeleted(BlobPath path, String name) throws Exception {
        assertBusy(() -> super.assertDeleted(path, name), 10L, TimeUnit.MINUTES);
    }
}
