package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.isNullOrEmpty;

public class DeprecationIndexingService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(DeprecationIndexingService.class);

    private static final String DATA_STREAM_NAME = "logs-deprecation-elasticsearch";
    private static final String TEMPLATE_NAME = DATA_STREAM_NAME + "-template";
    private static final String TEMPLATE_MAPPING = TEMPLATE_NAME + ".json";

    private static final String DEPRECATION_ORIGIN = "deprecation";

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_logs.write_to_index",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Client client;
    private boolean isEnabled = true;
    private boolean hasTriedToLoadTemplate = false;

    public DeprecationIndexingService(ClusterService clusterService, Client client) {
        this.client = new OriginSettingClient(client, DEPRECATION_ORIGIN);

        clusterService.addListener(this);
    }

    /**
     * Indexes a deprecation message.
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
        payload.put("key", key);
        payload.put("message", message);

        if (isNullOrEmpty(xOpaqueId) == false) {
            payload.put("x-opaque-id", xOpaqueId);
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

        if (this.isEnabled == false || this.hasTriedToLoadTemplate == true) {
            return;
        }

        // We only ever try to load the template once, because if there's a problem, we'll spam
        // the log with the failure on every cluster state update
        this.hasTriedToLoadTemplate = true;

        if (event.state().getMetadata().templatesV2().containsKey(TEMPLATE_NAME)) {
            return;
        }

        loadTemplate();
    }

    private void loadTemplate() {
        try (InputStream is = getClass().getResourceAsStream(TEMPLATE_MAPPING)) {
            final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, is);

            PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(TEMPLATE_NAME);
            request.cause("auto (deprecation indexing service)");
            request.indexTemplate(ComposableIndexTemplate.parse(parser));

            this.client.execute(PutComposableIndexTemplateAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged() == false) {
                        LOGGER.error("The attempt to create a deprecations index template was not acknowledged.");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Failed to create the deprecations index template: " + e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to load " + TEMPLATE_MAPPING + ": " + e.getMessage(), e);
        }
    }
}
