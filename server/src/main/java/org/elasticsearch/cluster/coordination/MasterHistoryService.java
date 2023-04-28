/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * This service provides access to this node's view of the master history, as well as access to other nodes' view of master stability.
 */
public class MasterHistoryService {
    private final TransportService transportService;
    private final MasterHistory localMasterHistory;
    private final LongSupplier currentTimeMillisSupplier;
    private final TimeValue acceptableRemoteHistoryAge;
    /*
     * This is a view of the master history one a remote node, or the exception that fetching it resulted in. This is populated
     * asynchronously. It is non-private for testing. Note that this field is not nulled out after its time to live expires. That check
     * is only done in getRemoteMasterHistory(). All non-testing access to this field needs to go through getRemoteMasterHistory().
     */
    volatile RemoteHistoryOrException remoteHistoryOrException = new RemoteHistoryOrException(null, null, Long.MIN_VALUE);
    private static final Logger logger = LogManager.getLogger(MasterHistoryService.class);

    private static final TimeValue DEFAULT_REMOTE_HISTORY_TIME_TO_LIVE = new TimeValue(5, TimeUnit.MINUTES);

    /**
     * This is the amount of time that can pass after a RemoteHistoryOrException is returned from the remote master until it is
     * considered stale and not usable.
     */
    public static final Setting<TimeValue> REMOTE_HISTORY_TIME_TO_LIVE_SETTING = Setting.positiveTimeSetting(
        "master_history.remote_history_time_to_live",
        DEFAULT_REMOTE_HISTORY_TIME_TO_LIVE,
        Setting.Property.NodeScope
    );

    public MasterHistoryService(TransportService transportService, ThreadPool threadPool, ClusterService clusterService) {
        this.transportService = transportService;
        this.localMasterHistory = new MasterHistory(threadPool, clusterService);
        this.currentTimeMillisSupplier = threadPool::relativeTimeInMillis;
        this.acceptableRemoteHistoryAge = REMOTE_HISTORY_TIME_TO_LIVE_SETTING.get(clusterService.getSettings());
    }

    /**
     * This returns the MasterHistory as seen from this node. The returned MasterHistory will be automatically updated whenever the
     * ClusterState on this node is updated with new information about the master.
     * @return The MasterHistory from this node's point of view. This MasterHistory object will be updated whenever the ClusterState changes
     */
    public MasterHistory getLocalMasterHistory() {
        return localMasterHistory;
    }

    /**
     * This method returns a static view of the MasterHistory on a remote node. This MasterHistory is static in that it will not be
     * updated even if the ClusterState is updated on this node or the remote node. The history is retrieved asynchronously, and only if
     * refreshRemoteMasterHistory has been called for this node. If anything has gone wrong fetching it, the exception returned by the
     * remote machine will be thrown here. If the remote history has not been fetched or if something went wrong and there was no exception,
     * the returned value will be null. If the remote history is old enough to be considered stale (that is, older than
     * MAX_USABLE_REMOTE_HISTORY_AGE_SETTING), then the returned value will be null.
     * @return The MasterHistory from a remote node's point of view. This MasterHistory object will not be updated with future changes
     * @throws Exception the exception (if any) returned by the remote machine when fetching the history
     */
    @Nullable
    public List<DiscoveryNode> getRemoteMasterHistory() throws Exception {
        // Grabbing a reference to the object in case it is replaced in another thread during this method:
        RemoteHistoryOrException remoteHistoryOrExceptionCopy = remoteHistoryOrException;
        /*
         * If the remote history we have is too old, we just return null with the assumption that it is stale and the new one has not
         * come in yet.
         */
        long acceptableRemoteHistoryTime = currentTimeMillisSupplier.getAsLong() - acceptableRemoteHistoryAge.getMillis();
        if (remoteHistoryOrExceptionCopy.creationTimeMillis < acceptableRemoteHistoryTime) {
            return null;
        }
        if (remoteHistoryOrExceptionCopy.exception != null) {
            throw remoteHistoryOrExceptionCopy.exception;
        }
        return remoteHistoryOrExceptionCopy.remoteHistory;
    }

    /**
     * This method attempts to fetch the master history from the requested node. If we are able to successfully fetch it, it will be
     * available in a later call to getRemoteMasterHistory. The client is not notified if or when the remote history is successfully
     * retrieved. This method only fetches the remote master history once, and it is never updated unless this method is called again. If
     * two calls are made to this method, the response of one will overwrite the response of the other (with no guarantee of the ordering
     * of responses).
     * This is a remote call, so clients should avoid calling it any more often than necessary.
     * @param node The node whose view of the master history we want to fetch
     */
    public void refreshRemoteMasterHistory(DiscoveryNode node) {
        Version minSupportedVersion = Version.V_8_4_0;
        if (node.getVersion().before(minSupportedVersion)) { // This was introduced in 8.3.0 (and the action name changed in 8.4.0)
            logger.trace(
                "Cannot get master history for {} because it is at version {} and {} is required",
                node,
                node.getVersion(),
                minSupportedVersion
            );
            return;
        }
        long startTime = System.nanoTime();
        transportService.connectToNode(
            // Note: This connection must be explicitly closed below
            node,
            new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    logger.trace("Connected to {}, making master history request", node);
                    // If we don't get a response in 10 seconds that is a failure worth capturing on its own:
                    final TimeValue remoteMasterHistoryTimeout = TimeValue.timeValueSeconds(10);
                    transportService.sendRequest(
                        node,
                        MasterHistoryAction.NAME,
                        new MasterHistoryAction.Request(),
                        TransportRequestOptions.timeout(remoteMasterHistoryTimeout),
                        new ActionListenerResponseHandler<>(ActionListener.runBefore(new ActionListener<>() {

                            @Override
                            public void onResponse(MasterHistoryAction.Response response) {
                                long endTime = System.nanoTime();
                                logger.trace("Received history from {} in {}", node, TimeValue.timeValueNanos(endTime - startTime));
                                remoteHistoryOrException = new RemoteHistoryOrException(
                                    response.getMasterHistory(),
                                    currentTimeMillisSupplier.getAsLong()
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Exception in master history request to master node", e);
                                remoteHistoryOrException = new RemoteHistoryOrException(e, currentTimeMillisSupplier.getAsLong());
                            }
                        }, () -> Releasables.close(releasable)), MasterHistoryAction.Response::new)
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Exception connecting to master node", e);
                    remoteHistoryOrException = new RemoteHistoryOrException(e, currentTimeMillisSupplier.getAsLong());
                }
            }
        );
    }

    // non-private for testing
    record RemoteHistoryOrException(List<DiscoveryNode> remoteHistory, Exception exception, long creationTimeMillis) {

        public RemoteHistoryOrException {
            if (remoteHistory != null && exception != null) {
                throw new IllegalArgumentException("Remote history and exception cannot both be non-null");
            }
        }

        RemoteHistoryOrException(List<DiscoveryNode> remoteHistory, long creationTimeMillis) {
            this(remoteHistory, null, creationTimeMillis);
        }

        RemoteHistoryOrException(Exception exception, long creationTimeMillis) {
            this(null, exception, creationTimeMillis);
        }
    }
}
