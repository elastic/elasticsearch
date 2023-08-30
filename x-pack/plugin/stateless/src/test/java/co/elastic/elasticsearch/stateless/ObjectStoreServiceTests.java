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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;
import co.elastic.elasticsearch.stateless.utils.TransferableCloseables;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.ObjectStoreService.BUCKET_SETTING;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.AZURE;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.FS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.GCS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.S3;
import static org.hamcrest.Matchers.equalTo;

public class ObjectStoreServiceTests extends ESTestCase {

    public void testNoBucket() {
        ObjectStoreType type = randomFrom(ObjectStoreType.values());
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE_SETTING.get(builder.build()));
    }

    public void testObjectStoreSettingsNoClient() {
        ObjectStoreType type = randomFrom(S3, GCS, AZURE);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        builder.put(BUCKET_SETTING.getKey(), randomAlphaOfLength(5));
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE_SETTING.get(builder.build()));
    }

    public void testFSSettings() {
        String bucket = randomAlphaOfLength(5);
        String basePath = randomBoolean() ? randomAlphaOfLength(5) : null;

        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), FS.name());
        builder.put(ObjectStoreService.BUCKET_SETTING.getKey(), bucket);
        if (basePath != null) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), basePath);
        }
        // no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.createRepositorySettings(bucket, randomAlphaOfLength(5), basePath);
        assertThat(settings.keySet().size(), equalTo(1));
        assertThat(settings.get("location"), equalTo(basePath != null ? PathUtils.get(bucket, basePath).toString() : bucket));
    }

    public void testObjectStoreSettings() {
        validateObjectStoreSettings(S3, "bucket");
        validateObjectStoreSettings(GCS, "bucket");
        validateObjectStoreSettings(AZURE, "container");
    }

    private void validateObjectStoreSettings(ObjectStoreType type, String bucketName) {
        String bucket = randomAlphaOfLength(5);
        String client = randomAlphaOfLength(5);
        String basePath = randomBoolean() ? randomAlphaOfLength(5) : null;

        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        builder.put(ObjectStoreService.BUCKET_SETTING.getKey(), bucket);
        builder.put(ObjectStoreService.CLIENT_SETTING.getKey(), client);
        if (basePath != null) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), basePath);
        }
        // check no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.createRepositorySettings(bucket, client, basePath);
        assertThat(settings.keySet().size(), equalTo(basePath != null ? 3 : 2));
        assertThat(settings.get(bucketName), equalTo(bucket));
        assertThat(settings.get("client"), equalTo(client));
        assertThat(settings.get("base_path"), equalTo(basePath));
    }

    public void testStartingShardRetrievesSegmentsFromOneCommit() throws IOException {
        final var mergesEnabled = randomBoolean();
        final var indexWriterConfig = mergesEnabled
            ? new IndexWriterConfig(new KeywordAnalyzer())
            : Lucene.indexWriterConfigWithNoMerging(new KeywordAnalyzer());

        final var permittedFiles = new HashSet<String>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public InputStream readBlob(String blobName) throws IOException {
                        assert StatelessCompoundCommit.startsWithBlobPrefix(blobName) || permittedFiles.contains(blobName)
                            : blobName + " in " + permittedFiles;
                        return super.readBlob(blobName);
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        }) {
            var commitCount = between(0, 5);

            try (
                var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig);
                var closeables = new TransferableCloseables()
            ) {

                Set<String> previousCommitFiles = Collections.emptySet();
                for (int commit = 0; commit < commitCount; commit++) {
                    indexWriter.addDocument(List.of());
                    indexWriter.forceMerge(1);
                    indexWriter.commit();
                    final var indexReader = closeables.add(DirectoryReader.open(indexWriter));
                    final var indexCommit = indexReader.getIndexCommit();
                    final var commitFiles = testHarness.indexingStore.getMetadata(indexCommit).fileMetadataMap().keySet();
                    ;
                    final var additionalFiles = Sets.difference(commitFiles, previousCommitFiles);
                    previousCommitFiles = commitFiles;
                    if (commit == 0 || mergesEnabled == false) {
                        final var segmentCommitInfos = SegmentInfos.readCommit(
                            testHarness.indexingDirectory,
                            indexCommit.getSegmentsFileName()
                        );
                        assertEquals(commit + 1, segmentCommitInfos.size());
                        for (SegmentCommitInfo segmentCommitInfo : segmentCommitInfos) {
                            assertTrue(segmentCommitInfo.info.getUseCompoundFile());
                        }
                    }

                    permittedFiles.clear();
                    permittedFiles.addAll(indexCommit.getFileNames());

                    PlainActionFuture.<Void, IOException>get(
                        future -> testHarness.commitService.onCommitCreation(
                            new StatelessCommitRef(
                                testHarness.shardId,
                                new Engine.IndexCommitRef(indexCommit, () -> future.onResponse(null)),
                                commitFiles,
                                additionalFiles,
                                1,
                                0
                            )
                        ),
                        10,
                        TimeUnit.SECONDS
                    );
                }
            }

            assertEquals(
                commitCount,
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId, 1)
                    .listBlobs()
                    .keySet()
                    .stream()
                    .filter(StatelessCompoundCommit::startsWithBlobPrefix)
                    .count()
            );

            final var dir = SearchDirectory.unwrapDirectory(testHarness.searchStore.directory());
            dir.setBlobContainer(primaryTerm -> testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm));
            StatelessCompoundCommit commit = ObjectStoreService.readSearchShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                1
            );
            if (commit != null) {
                dir.updateCommit(commit);
            }

            if (commitCount > 0) {
                assertEquals(permittedFiles, Set.of(dir.listAll()));
            }

            try (var indexReader = DirectoryReader.open(testHarness.searchStore.directory())) {
                assertEquals(commitCount, indexReader.numDocs());
            }
        }
    }
}
