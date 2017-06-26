/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.template.TemplateUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Registry for the Logstash index template and settings
 * This class is based on xpack.security.SecurityLifecycleService.
 */
public class LogstashTemplateRegistry extends AbstractComponent implements ClusterStateListener {

    public static final String LOGSTASH_INDEX_NAME = ".logstash";
    public static final String LOGSTASH_TEMPLATE_NAME = "logstash-index-template";
    public static final String TEMPLATE_VERSION_PATTERN =
        Pattern.quote("${logstash.template.version}");

    private static final String LOGSTASH_VERSION_PROPERTY = "logstash-version";

    private final Client client;

    private final AtomicBoolean templateIsUpToDate = new AtomicBoolean(false);

    // only put the template if this is not already in progress
    private final AtomicBoolean templateCreationPending = new AtomicBoolean(false);

    public LogstashTemplateRegistry(Settings settings, ClusterService clusterService, Client client) {
        super(settings);
        this.client = client;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {

            // wait until the gateway has recovered from disk,
            // otherwise we think may not have the index templates while they actually do exist
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                addTemplatesIfMissing(event.state());
            }
        }
    }

    public boolean isTemplateUpToDate() {
        return templateIsUpToDate.get();
    }

    public boolean isTemplateCreationPending() {
        return templateCreationPending.get();
    }

    private void addTemplatesIfMissing(ClusterState state) {
        this.templateIsUpToDate.set(TemplateUtils.checkTemplateExistsAndIsUpToDate(LOGSTASH_TEMPLATE_NAME,
            LOGSTASH_VERSION_PROPERTY, state, logger));

        // only put the template if its not up to date and if its not already in progress
        if (isTemplateUpToDate() == false && templateCreationPending.compareAndSet(false, true)) {
            putTemplate();
        }
    }

    private void putTemplate() {
        logger.debug("putting the template [{}]", LOGSTASH_TEMPLATE_NAME);
        String template = TemplateUtils.loadTemplate("/" + LOGSTASH_TEMPLATE_NAME + ".json",
            Version.CURRENT.toString(), TEMPLATE_VERSION_PATTERN);

        PutIndexTemplateRequest putTemplateRequest = client.admin().indices()
            .preparePutTemplate(LOGSTASH_TEMPLATE_NAME)
            .setSource(
                new BytesArray(template.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON)
            .request();

        client.admin().indices().putTemplate(putTemplateRequest, ActionListener.wrap(r -> {
            templateCreationPending.set(false);
            if (r.isAcknowledged()) {
                templateIsUpToDate.set(true);
                logger.debug("successfully updated [{}] index template", LOGSTASH_TEMPLATE_NAME);
            } else {
                logger.error("put template [{}] was not acknowledged", LOGSTASH_TEMPLATE_NAME);
            }
        }, e -> {
            templateCreationPending.set(false);
            logger.warn(new ParameterizedMessage(
                "failed to put template [{}]", LOGSTASH_TEMPLATE_NAME), e);
        }));
    }
}
