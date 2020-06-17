package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.isNullOrEmpty;

public class DeprecationIndexingService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(DeprecationIndexingService.class);
    private static final String DATA_STREAM_NAME = "deprecation-elasticsearch-default";
    private static final String DEPRECATION_ORIGIN = "deprecation";

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_logs.write_to_index",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Client client;
    private boolean isEnabled = true;

    public DeprecationIndexingService(ClusterService clusterService, Client client) {
        this.client = new OriginSettingClient(client, DEPRECATION_ORIGIN);

        clusterService.addListener(this);

        // TODO create data stream template
    }

    /**
     * Indexes a deprecation message in an ECS format
     *
     * @param key       the key that was used to determine if this deprecation should have been be logged.
     *                  Useful when aggregating the recorded messages.
     * @param message   the message to log
     * @param xOpaqueId the associated "X-Opaque-ID" header value, if any
     * @param params    parameters to the message, if any
     */
    public void writeMessage(String key, String message, String xOpaqueId, Object[] params) {
        if (this.isEnabled == false) {
            return;
        }

        Map<String, Object> payload = new HashMap<>();

        payload.put("@timestamp", Instant.now().toString());
        payload.put("message", message);
        payload.put("tags", new String[] { key });

        if (isNullOrEmpty(xOpaqueId) == false) {
            // This seems to be the most appropriate location. There's also `transaction.id`
            // Or would it be clearer to just use 'x-opaque-id'?
            payload.put("trace.id", xOpaqueId);
        }

        if (params != null && params.length > 0) {
            payload.put("params", params);
        }

        new IndexRequestBuilder(client, IndexAction.INSTANCE).setIndex(DATA_STREAM_NAME)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(payload)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    // Nothing to do
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Failed to index deprecation message", e);
                }
            });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        this.isEnabled = WRITE_DEPRECATION_LOGS_TO_INDEX.get(event.state().getMetadata().settings());
    }
}
