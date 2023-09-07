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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;

import java.util.function.Supplier;

public class IndicesMappingSizePublisher {

    private static final TransportVersion REQUIRED_VERSION = TransportVersions.V_8_500_050;

    private final NodeClient client;
    private final Supplier<TransportVersion> minimumTransportVersion;

    public IndicesMappingSizePublisher(final Client client, final Supplier<TransportVersion> minimumTransportVersion) {
        this.client = (NodeClient) client;
        this.minimumTransportVersion = minimumTransportVersion;
    }

    public void publishIndicesMappingSize(final HeapMemoryUsage heapMemoryUsage, final ActionListener<ActionResponse.Empty> listener) {
        final TransportVersion minimumClusterVersion = minimumTransportVersion.get();
        if (minimumClusterVersion.onOrAfter(REQUIRED_VERSION)) {
            var request = new PublishHeapMemoryMetricsRequest(heapMemoryUsage);
            client.execute(PublishHeapMemoryMetricsAction.INSTANCE, request, listener);
        } else {
            listener.onFailure(
                new ElasticsearchException(
                    "Cannot publish indices mapping size metric until entire cluster is: ["
                        + REQUIRED_VERSION
                        + "], found: ["
                        + minimumClusterVersion
                        + "]"
                )
            );
        }
    }
}
