package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Function;

/**
 * <p>
 * This is a filter snapshot repository that only snapshots the minimal required information
 * that is needed to recreate the index. In other words instead of snapshotting the entire shard
 * with all it's lucene indexed fields, doc values, points etc. it only snapshots the the stored
 * fields including _source and _routing as well as the live docs in oder to distinguish between
 * live and deleted docs.
 * </p>
 * <p>
 * The repository can wrap any other repository delegating the source only snapshot to it to and read
 * from it. For instance a file repository of type <tt>fs</tt> by passing <tt>settings.delegate_type=fs</tt>
 * at repository creation time.
 * </p>
 * The repository supports two distinct restore options:
 * <ul>
 *     <li><b>minimal restore</b>: this option re-indexes all documents during restore with an empty mapping. The original mapping is
 *     stored in the restored indexes <tt>_meta</tt> mapping field. The <tt>minimal</tt> restore must be enabled by setting
 *     <tt>settings.restore_minimal=true</tt>.</li>
 *     <li><b>full restore</b>: this option re-indexes all documents during restore with the original mapping. This option is the
 *     default. This option has a significant operational overhead compared to the minimal option but recreates a fully functional new
 *     index</li>
 * </ul>
 *
 * Reindex operations are executed in a single thread and can be monitored via indices recovery stats. Every indexed document will be
 * reported as a translog document.
 *
 */
// TODO: as a followup we should rename translog phase to operation phase in the indices _recovery stats
public final class SourceOnlySnapshotRepository extends FilterRepository {
    public static final Setting<String> DELEGATE_TYPE =
        new Setting<>("delegate_type", "", Function.identity(), Setting.Property.NodeScope);

    public static final Setting<Boolean> RESTORE_MINIMAL = Setting.boolSetting("restore_minimal",
        false, Setting.Property.NodeScope);

    public static final String SNAPSHOT_DIR_NAME = "_snapshot";
    public static final String RESTORE_DIR_NAME = "_restore";
    private final boolean restoreMinimal;

    public static Repository.Factory newFactory() {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                boolean restoreMinimal = RESTORE_MINIMAL.get(metaData.settings());
                Repository.Factory factory = typeLookup.apply(delegateType);
                return new SourceOnlySnapshotRepository(factory.create(new RepositoryMetaData(metaData.name(),
                    delegateType, metaData.settings()), typeLookup), restoreMinimal);
            }
        };
    }

    public SourceOnlySnapshotRepository(Repository in, boolean restoreMinimal) {
        super(in);
        this.restoreMinimal = restoreMinimal;
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        IndexMetaData snapshotIndexMetaData = super.getSnapshotIndexMetaData(snapshotId, index);
        if (restoreMinimal) {
            // TODO: can we lie about the index.version.created here and produce an index with a new version since we reindex anyway?

            // for a minimal restore we basically disable indexing on all fields and only create an index
            // that is fully functional from an operational perspective. ie. it will have all metadata fields like version/
            // seqID etc. and an indexed ID field such that we can potentially perform updates on them or delete documents.
            ImmutableOpenMap<String, MappingMetaData> mappings = snapshotIndexMetaData.getMappings();
            Iterator<ObjectObjectCursor<String, MappingMetaData>> iterator = mappings.iterator();
            IndexMetaData.Builder builder = IndexMetaData.builder(snapshotIndexMetaData);
            while (iterator.hasNext()) {
                ObjectObjectCursor<String, MappingMetaData> next = iterator.next();
                MappingMetaData.Routing routing = next.value.routing();
                final String mapping;
                if (routing.required()) { // we have to respect the routing to be on the safe side so we pass this one on.
                    mapping = "{ \"" + next.key + "\": { \"enabled\": false, \"_meta\": " + next.value.source().string() + ", " +
                        "\"_routing\" : { \"required\" : true } } }";
                } else {
                    mapping = "{ \"" + next.key + "\": { \"enabled\": false, \"_meta\": " + next.value.source().string() + " } }";
                }
                System.out.println(mapping);
                builder.putMapping(next.key, mapping);
            }
            return builder.build();
        } else {
            return snapshotIndexMetaData;
        }

    }

    @Override
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus) {
        if (shard.mapperService().documentMapper() != null // if there is no mapping this is null
            && shard.mapperService().documentMapper().sourceMapper().isComplete() == false) {
            throw new IllegalStateException("Can't snapshot _source only on an index that has incomplete source ie. has _source disabled " +
                "or filters the source");
        }
        ShardPath shardPath = shard.shardPath();
        Path dataPath = shardPath.getDataPath();
        // TODO should we have a snapshot tmp directory per shard that is maintained by the system?
        Path snapPath = dataPath.resolve(SNAPSHOT_DIR_NAME);
        try (FSDirectory directory = new SimpleFSDirectory(snapPath)) {
            Store tempStore = new Store(store.shardId(), store.indexSettings(), directory, new ShardLock(store.shardId()) {
                @Override
                protected void closeInternal() {
                    // do nothing;
                }
            }, Store.OnClose.EMPTY);
            SourceOnlySnapshot snapshot = new SourceOnlySnapshot(tempStore.directory(), null);
            snapshot.syncSnapshot(snapshotIndexCommit);
            store.incRef();
            try (DirectoryReader reader = DirectoryReader.open(tempStore.directory());
                 ) {
                IndexCommit indexCommit = reader.getIndexCommit();
                super.snapshotShard(shard, tempStore, snapshotId, indexId, indexCommit, snapshotStatus);
            } finally {
                store.decRef();
            }
        } catch (IOException e) {
            // why on earth does this super method not declare IOException
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
        super.restoreShard(shard, snapshotId, version, indexId, snapshotShardId, recoveryState);
        ShardPath shardPath = shard.shardPath();
        try {
            Path restoreSourceCopy = shardPath.getDataPath().resolve(RESTORE_DIR_NAME);
            try (HardlinkCopyDirectoryWrapper wrapper = new HardlinkCopyDirectoryWrapper(FSDirectory.open(restoreSourceCopy))) {
                Lucene.cleanLuceneIndex(wrapper);
                SegmentInfos segmentInfos = shard.store().readLastCommittedSegmentsInfo();
                for (String file : segmentInfos.files(true)) {
                    wrapper.copyFrom(shard.store().directory(), file, file, IOContext.DEFAULT);
                }
            }
            Lucene.cleanLuceneIndex(shard.store().directory()); // wipe the old index
            shard.store().createEmpty();
        } catch (IOException ex) {
            // why on earth does this super method not declare IOException
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void applyPostRestoreOps(IndexShard shard) throws IOException {
        ShardPath shardPath = shard.shardPath();
        Path restoreSourceCopy = shardPath.getDataPath().resolve(RESTORE_DIR_NAME);
        RecoveryState.Translog state = shard.recoveryState().getTranslog();
        assert state.totalOperations() == 0 : "translog state should have 0 total ops but got: " + state.totalOperations();
        state.reset();
        long sequenceNumber = 0;
        try (FSDirectory dir = FSDirectory.open(restoreSourceCopy)) {
            try (IndexReader reader = DirectoryReader.open(dir)) {
                state.totalOperationsOnStart(reader.numDocs());
                state.totalOperations(reader.numDocs());
                long primaryTerm = shard.getPendingPrimaryTerm();
                FieldsVisitor rootFieldsVisitor = new FieldsVisitor(true);
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    Bits liveDocs = leafReader.getLiveDocs();
                    // TODO: we could do this in parallel per segment here or even per docID
                    // there is a lot of room for doing this multi-threaded but not for the first iteration
                    for (int i = 0; i < leafReader.maxDoc(); i++) {
                        if (liveDocs == null || liveDocs.get(i)) {
                            rootFieldsVisitor.reset();
                            leafReader.document(i, rootFieldsVisitor);
                            rootFieldsVisitor.postProcess(shard.mapperService());
                            Uid uid = rootFieldsVisitor.uid();
                            BytesReference source = rootFieldsVisitor.source();
                            if (source != null) { // nested fields don't have source. in this case we should be fine.
                                // TODO we should have a dedicated origin for this LOCAL_TRANSLOG_RECOVERY is misleading.
                                Engine.Result result = shard.applyTranslogOperation(new Translog.Index(uid.type(), uid.id(),
                                    sequenceNumber++, primaryTerm, 1, source.toBytesRef().bytes,
                                    rootFieldsVisitor.routing(), -1), Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY);
                                if (result.getResultType() != Engine.Result.Type.SUCCESS) {
                                    throw new IllegalStateException("failed applying post restore operation result: " + result
                                        .getResultType(), result.getFailure());
                                }
                                state.incrementRecoveredOperations();
                            } else {
                                assert restoreMinimal // in this case we don't have nested in the mapping.
                                    || shard.mapperService().hasNested() : "_source is null but shard has no nested docs";
                            }
                        }
                    }
                }
                shard.flush(new FlushRequest());
            }
            Lucene.cleanLuceneIndex(dir); // clear the tmp index;
        }
    }
}
