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

package co.elastic.elasticsearch.stateless.upgrade;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;

public class StatelessUpgrader implements ClusterStateListener {

    public static final TransportVersion RETRY_ALLOCATION_VERSION = TransportVersion.V_8_500_036;
    private static final Logger logger = LogManager.getLogger(StatelessUpgrader.class);

    private final Client client;
    private final ClusterService clusterService;

    public StatelessUpgrader(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // If we are the master that published the cluster state which advanced the min transport version past the one with the bug then we
        // also (effectively) do `POST _cluster/reroute?retry_failed` to retry the allocation of any shards that ran out of retries due to
        // the bug. This works even if we stop being the master before the reroute takes effect, because this action will retry on the new
        // master. It is, however, not completely watertight: if this node dies or the cluster takes more than 24h to elect another master
        // then the reroute will fail and no other node will try to do this reroute again. We accept the risk that there may be a tiny
        // number of clusters that need someone to manually do the reroute; if it's not a tiny number of such clusters then that's another
        // bug.

        if (event.previousState().getMinTransportVersion().before(RETRY_ALLOCATION_VERSION)
            && event.state().getMinTransportVersion().onOrAfter(RETRY_ALLOCATION_VERSION)) {
            if (event.localNodeMaster()) {
                logger.debug(
                    "attempting automatic retry allocation of unassigned shards after cluster upgraded to: "
                        + event.state().getMinTransportVersion()
                );
                client.admin()
                    .cluster()
                    .prepareReroute()
                    .setMasterNodeTimeout(TimeValue.timeValueHours(24))
                    .setRetryFailed(true)
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterRerouteResponse clusterRerouteResponse) {
                            logger.debug("automatic retry allocation of unassigned shards finished");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(
                                "automatic allocation of unassigned shards failed, manually run POST "
                                    + "_cluster/reroute?retry_failed&metric=none to retry",
                                e
                            );
                        }
                    });
            }
            clusterService.removeListener(this);
        }
    }
}
