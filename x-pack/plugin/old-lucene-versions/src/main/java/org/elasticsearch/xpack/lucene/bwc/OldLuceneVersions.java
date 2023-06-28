/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.sourceonly.SourceOnlySnapshotRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class OldLuceneVersions extends Plugin implements IndexStorePlugin, ClusterPlugin, RepositoryPlugin, ActionPlugin, EnginePlugin {

    public static final LicensedFeature.Momentary ARCHIVE_FEATURE = LicensedFeature.momentary(
        null,
        "archive",
        License.OperationMode.ENTERPRISE
    );

    private static Version MINIMUM_ARCHIVE_VERSION = Version.fromString("5.0.0");

    private final SetOnce<FailShardsOnInvalidLicenseClusterListener> failShardsListener = new SetOnce<>();

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry registry,
        final IndexNameExpressionResolver resolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        this.failShardsListener.set(new FailShardsOnInvalidLicenseClusterListener(getLicenseState(), clusterService.getRerouteService()));
        if (DiscoveryNode.isMasterNode(environment.settings())) {
            // We periodically look through the indices and identify if there are any archive indices,
            // then marking the feature as used. We do this on each master node so that if one master fails, the
            // continue reporting usage state.
            var usageTracker = new ArchiveUsageTracker(getLicenseState(), clusterService::state);
            threadPool.scheduleWithFixedDelay(usageTracker, TimeValue.timeValueMinutes(15), ThreadPool.Names.GENERIC);
        }
        return List.of();
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.ARCHIVE, ArchiveUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.ARCHIVE, ArchiveInfoTransportAction.class)
        );
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new ArchiveAllocationDecider(() -> ARCHIVE_FEATURE.checkWithoutTracking(getLicenseState())));
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.indexSettings().getIndexVersionCreated().isLegacyIndexVersion()) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void afterFilesRestoredFromRepository(IndexShard indexShard) {
                    convertToNewFormat(indexShard);
                }
            });

            indexModule.addIndexEventListener(failShardsListener.get());

            indexModule.addSettingsUpdateConsumer(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, s -> {}, write -> {
                if (write == false) {
                    throw new IllegalArgumentException("Cannot remove write block from archive index");
                }
            });
        }
    }

    @Override
    public BiConsumer<Snapshot, Version> addPreRestoreVersionCheck() {
        return (snapshot, version) -> {
            if (version.isLegacyIndexVersion()) {
                if (ARCHIVE_FEATURE.checkWithoutTracking(getLicenseState()) == false) {
                    throw LicenseUtils.newComplianceException("archive");
                }
                if (version.before(MINIMUM_ARCHIVE_VERSION)) {
                    throw new SnapshotRestoreException(
                        snapshot,
                        "the snapshot was created with Elasticsearch version ["
                            + version
                            + "] which isn't supported by the archive functionality"
                    );
                }
            }
        };
    }

    /**
     * The trick used to allow newer Lucene versions to read older Lucene indices is to convert the old directory to a directory that new
     * Lucene versions happily operate on. The way newer Lucene versions happily comply with reading older data is to put in place a
     * segments file that the newer Lucene version can open, using codecs that allow reading everything from the old files, making it
     * available under the newer interfaces. The way this works is to read in the old segments file using a special class
     * {@link OldSegmentInfos} that supports reading older Lucene {@link SegmentInfos}, and then write out an updated segments file that
     * newer Lucene versions can understand.
     */
    private static void convertToNewFormat(IndexShard indexShard) {
        indexShard.store().incRef();
        try {
            final OldSegmentInfos oldSegmentInfos = OldSegmentInfos.readLatestCommit(indexShard.store().directory(), 6);
            final SegmentInfos segmentInfos = convertToNewerLuceneVersion(oldSegmentInfos);
            // write upgraded segments file
            segmentInfos.commit(indexShard.store().directory());

            // what we have written can be read using standard path
            assert SegmentInfos.readLatestCommit(indexShard.store().directory()) != null;

            // clean older segments file
            Lucene.pruneUnreferencedFiles(segmentInfos.getSegmentsFileName(), indexShard.store().directory());
        } catch (IOException e) {
            throw new UncheckedIOException(
                Strings.format(
                    """
                        Elasticsearch version [{}] has limited support for indices created in version [{}] but this index could not be \
                        read. It may be using an unsupported feature, or it may be damaged or corrupt. See {} for further information.""",
                    Version.CURRENT,
                    IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexShard.indexSettings().getSettings()),
                    ReferenceDocs.ARCHIVE_INDICES
                ),
                e
            );
        } finally {
            indexShard.store().decRef();
        }
    }

    private static SegmentInfos convertToNewerLuceneVersion(OldSegmentInfos oldSegmentInfos) {
        final SegmentInfos segmentInfos = new SegmentInfos(org.apache.lucene.util.Version.LATEST.major);
        segmentInfos.version = oldSegmentInfos.version;
        segmentInfos.counter = oldSegmentInfos.counter;
        segmentInfos.setNextWriteGeneration(oldSegmentInfos.getGeneration() + 1);
        final Map<String, String> map = new HashMap<>(oldSegmentInfos.getUserData());
        if (map.containsKey(Engine.HISTORY_UUID_KEY) == false) {
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
        }
        if (map.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) == false) {
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        }
        if (map.containsKey(SequenceNumbers.MAX_SEQ_NO) == false) {
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        }
        if (map.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) == false) {
            map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
        }
        segmentInfos.setUserData(map, false);
        for (SegmentCommitInfo infoPerCommit : oldSegmentInfos.asList()) {
            final SegmentInfo newInfo = BWCCodec.wrap(infoPerCommit.info);
            final SegmentCommitInfo commitInfo = new SegmentCommitInfo(
                newInfo,
                infoPerCommit.getDelCount(),
                infoPerCommit.getSoftDelCount(),
                infoPerCommit.getDelGen(),
                infoPerCommit.getFieldInfosGen(),
                infoPerCommit.getDocValuesGen(),
                infoPerCommit.getId()
            );
            commitInfo.setDocValuesUpdatesFiles(infoPerCommit.getDocValuesUpdatesFiles());
            commitInfo.setFieldInfosFiles(infoPerCommit.getFieldInfosFiles());
            segmentInfos.add(commitInfo);
        }
        return segmentInfos;
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of();
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getIndexVersionCreated().isLegacyIndexVersion()
            && indexSettings.getIndexMetadata().isSearchableSnapshot() == false
            && indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY) == false) {
            return Optional.of(
                engineConfig -> new ReadOnlyEngine(engineConfig, null, new TranslogStats(), true, Function.identity(), true, false)
            );
        }

        return Optional.empty();
    }
}
