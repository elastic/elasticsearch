/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Creates all data streams that are required for using Elastic Universal Profiling.
 */
public class ProfilingDataStreamManager extends AbstractProfilingPersistenceManager<ProfilingDataStreamManager.ProfilingDataStream> {
    public static final List<ProfilingDataStream> PROFILING_DATASTREAMS;

    static {
        List<ProfilingDataStream> dataStreams = new ArrayList<>(
            EventsIndex.indexNames()
                .stream()
                .map(n -> ProfilingDataStream.of(n, ProfilingIndexTemplateRegistry.PROFILING_EVENTS_VERSION))
                .toList()
        );
        dataStreams.add(ProfilingDataStream.of("profiling-metrics", ProfilingIndexTemplateRegistry.PROFILING_METRICS_VERSION));
        dataStreams.add(ProfilingDataStream.of("profiling-hosts", ProfilingIndexTemplateRegistry.PROFILING_HOSTS_VERSION));
        PROFILING_DATASTREAMS = Collections.unmodifiableList(dataStreams);
    }

    public ProfilingDataStreamManager(
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        IndexStateResolver indexStateResolver
    ) {
        super(threadPool, client, clusterService, indexStateResolver);
    }

    @Override
    protected void onIndexState(
        ClusterState clusterState,
        IndexState<ProfilingDataStream> indexState,
        ActionListener<? super ActionResponse> listener
    ) {
        IndexStatus status = indexState.getStatus();
        switch (status) {
            case NEEDS_CREATION -> createDataStream(indexState.getIndex(), listener);
            case NEEDS_VERSION_BUMP -> rolloverDataStream(indexState.getIndex(), listener);
            case NEEDS_MAPPINGS_UPDATE -> applyMigrations(indexState, listener);
            default -> {
                logger.trace("Skipping status change [{}] for data stream [{}].", status, indexState.getIndex());
                // ensure that listener is notified we're done
                listener.onResponse(null);
            }
        }
    }

    @Override
    protected Iterable<ProfilingDataStream> getManagedIndices() {
        return PROFILING_DATASTREAMS;
    }

    private void onDataStreamFailure(ProfilingDataStream dataStream, Exception ex) {
        logger.error(() -> format("error for data stream [%s] for [%s]", dataStream, ClientHelper.PROFILING_ORIGIN), ex);
    }

    private void rolloverDataStream(final ProfilingDataStream dataStream, ActionListener<? super ActionResponse> listener) {
        logger.debug("rolling over data stream [{}].", dataStream);
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            RolloverRequest request = new RolloverRequest(dataStream.getName(), null);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                new ActionListener<RolloverResponse>() {
                    @Override
                    public void onResponse(RolloverResponse response) {
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error rolling over data stream [{}] for [{}], request was not acknowledged",
                                dataStream,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isShardsAcknowledged() == false) {
                            logger.warn(
                                "rolling over data stream [{}] for [{}], shards were not acknowledged",
                                dataStream,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isRolledOver() == false) {
                            logger.warn("could not rollover data stream [{}] for [{}].", dataStream, ClientHelper.PROFILING_ORIGIN);
                        } else {
                            logger.debug(
                                "rolled over data stream [{}] from [{}] to index [{}] for [{}].",
                                dataStream,
                                response.getOldIndex(),
                                response.getNewIndex(),
                                ClientHelper.PROFILING_ORIGIN
                            );
                        }
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onDataStreamFailure(dataStream, e);
                        listener.onFailure(e);
                    }
                },
                (req, l) -> client.admin().indices().rolloverIndex(req, l)
            );
        });
    }

    private void createDataStream(ProfilingDataStream dataStream, final ActionListener<? super ActionResponse> listener) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(
                TimeValue.ONE_MINUTE /* TODO should we wait longer? */,
                TimeValue.THIRTY_SECONDS /* TODO should we wait longer? */,
                dataStream.getName()
            );
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding data stream [{}] for [{}], request was not acknowledged",
                                dataStream,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        }
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onDataStreamFailure(dataStream, e);
                        listener.onFailure(e);
                    }
                },
                (req, l) -> client.execute(CreateDataStreamAction.INSTANCE, req, l)
            );
        });
    }

    /**
     * A datastream that is used by Universal Profiling.
     */
    static class ProfilingDataStream implements ProfilingIndexAbstraction {
        private final String name;
        private final int version;
        private final List<Migration> migrations;

        public static ProfilingDataStream of(String name, int version) {
            return of(name, version, null);
        }

        public static ProfilingDataStream of(String name, int version, Migration.Builder builder) {
            List<Migration> migrations = builder != null ? builder.build(version) : null;
            return new ProfilingDataStream(name, version, migrations);
        }

        private ProfilingDataStream(String name, int version, List<Migration> migrations) {
            this.name = name;
            this.version = version;
            this.migrations = migrations;
        }

        public ProfilingDataStream withVersion(int version) {
            return new ProfilingDataStream(name, version, migrations);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public List<Migration> getMigrations(int currentIndexTemplateVersion) {
            return migrations != null
                ? migrations.stream().filter(m -> m.getTargetIndexTemplateVersion() > currentIndexTemplateVersion).toList()
                : Collections.emptyList();
        }

        @Override
        public IndexMetadata indexMetadata(ClusterState state) {
            Map<String, DataStream> dataStreams = state.metadata().dataStreams();
            if (dataStreams == null) {
                return null;
            }
            DataStream ds = dataStreams.get(this.getName());
            if (ds == null) {
                return null;
            }
            Index writeIndex = ds.getWriteIndex();
            if (writeIndex == null) {
                return null;
            }
            return state.metadata().index(writeIndex);
        }

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProfilingDataStream that = (ProfilingDataStream) o;
            return version == that.version && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }
    }

    public static boolean isAllResourcesCreated(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingDataStream profilingDataStream : PROFILING_DATASTREAMS) {
            if (indexStateResolver.getIndexState(state, profilingDataStream).getStatus() != IndexStatus.UP_TO_DATE) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAnyResourceTooOld(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingDataStream profilingDataStream : PROFILING_DATASTREAMS) {
            if (indexStateResolver.getIndexState(state, profilingDataStream).getStatus() == IndexStatus.TOO_OLD) {
                return true;
            }
        }
        return false;
    }
}
