/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.Plugin.PluginServices;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IngestAttachmentPlugin extends Plugin implements IngestPlugin {

    /**
     * URL of an external tika-server to delegate document extraction to.
     * When set, the attachment processor will send documents to this server over HTTP instead of
     * running Apache Tika in-process. Required in stateless deployments.
     * Example: {@code http://tika-server:9998}
     */
    public static final Setting<String> TIKA_SERVER_URL_SETTING = Setting.simpleString(
        "ingest.attachment.tika_server.url",
        Setting.Property.NodeScope
    );

    /**
     * Per-request timeout for tika-server HTTP calls. Defaults to 60 seconds.
     */
    public static final Setting<TimeValue> TIKA_SERVER_REQUEST_TIMEOUT_SETTING = Setting.timeSetting(
        "ingest.attachment.tika_server.timeout",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    /**
     * Connection timeout for the tika-server HTTP client. Defaults to 5 seconds.
     */
    public static final Setting<TimeValue> TIKA_SERVER_CONNECT_TIMEOUT_SETTING = Setting.timeSetting(
        "ingest.attachment.tika_server.connect_timeout",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    /**
     * Internal escape hatch: when {@code true}, a stateless node is allowed to start without a
     * tika-server URL configured, falling back to in-process Tika extraction. This bypasses the
     * {@link TikaServerBootstrapCheck} that would otherwise prevent startup.
     *
     * <p><strong>This setting is for Elastic internal use only.</strong> It exists so that the
     * platform team can temporarily revert to in-JVM extraction if the tika-server deployment is
     * broken. It must never be exposed to or set by end users.
     */
    static final Setting<Boolean> ALLOW_LOCAL_IN_STATELESS_SETTING = Setting.boolSetting(
        "ingest.attachment.tika_server.allow_local_in_stateless",
        false,
        Setting.Property.NodeScope
    );

    private final SetOnce<AttachmentIngestMetrics> attachmentMetrics = new SetOnce<>();
    private final SetOnce<ExtractionBackend> extractionBackend = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            AttachmentProcessor.MAX_FIELD_SIZE_SETTING,
            AttachmentProcessor.MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING,
            TIKA_SERVER_URL_SETTING,
            TIKA_SERVER_REQUEST_TIMEOUT_SETTING,
            TIKA_SERVER_CONNECT_TIMEOUT_SETTING,
            ALLOW_LOCAL_IN_STATELESS_SETTING
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        attachmentMetrics.set(new AttachmentIngestMetrics(services.telemetryProvider().getMeterRegistry()));

        String tikaServerUrl = TIKA_SERVER_URL_SETTING.get(services.environment().settings());
        if (tikaServerUrl.isEmpty() == false) {
            Duration requestTimeout = Duration.ofMillis(
                TIKA_SERVER_REQUEST_TIMEOUT_SETTING.get(services.environment().settings()).millis()
            );
            Duration connectTimeout = Duration.ofMillis(
                TIKA_SERVER_CONNECT_TIMEOUT_SETTING.get(services.environment().settings()).millis()
            );
            extractionBackend.set(new TikaServerExtractionBackend(URI.create(tikaServerUrl), requestTimeout, connectTimeout));
        } else {
            extractionBackend.set(new LocalExtractionBackend());
        }

        return super.createComponents(services);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(
            AttachmentProcessor.TYPE,
            new AttachmentProcessor.Factory(parameters.env.settings(), attachmentMetrics, extractionBackend)
        );
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        return List.of(new TikaServerBootstrapCheck());
    }

    @Override
    public void close() throws IOException {
        ExtractionBackend backend = extractionBackend.get();
        if (backend != null) {
            backend.close();
        }
    }
}
