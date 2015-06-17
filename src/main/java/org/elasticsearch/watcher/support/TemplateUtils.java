/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 */
public class TemplateUtils extends AbstractComponent {

    private final ClientProxy client;

    @Inject
    public TemplateUtils(Settings settings, ClientProxy client) {
        super(settings);
        this.client = client;
    }

    /**
     * Resolves the template with the specified templateName from the classpath, optionally adds extra settings and
     * puts the index template into the cluster.
     *
     * This method blocks until the template has been created.
     */
    public void putTemplate(String templateName, Settings customSettings) {
        try (InputStream is = WatchStore.class.getResourceAsStream("/" + templateName + ".json")) {
            if (is == null) {
                throw new FileNotFoundException("Resource [/" + templateName + ".json] not found in classpath");
            }
            final byte[] template;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Streams.copy(is, out);
                template = out.bytes().toBytes();
            }
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(template);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        .build();
                request.settings(updatedSettings);
            }
            PutIndexTemplateResponse response = client.putTemplate(request);
        } catch (Exception e) {
            // throwing an exception to stop exporting process - we don't want to send data unless
            // we put in the template for it.
            throw new WatcherException("failed to load [{}.json]", e, templateName);
        }
    }

}
