/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.WebhookService;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.core.Strings.format;

public final class ReportingAttachmentParser implements EmailAttachmentParser<ReportingAttachment> {

    public static final String TYPE = "reporting";

    // total polling of 10 minutes happens this way by default
    static final Setting<TimeValue> INTERVAL_SETTING = Setting.timeSetting(
        "xpack.notification.reporting.interval",
        TimeValue.timeValueSeconds(15),
        Setting.Property.NodeScope
    );
    static final Setting<Integer> RETRIES_SETTING = Setting.intSetting(
        "xpack.notification.reporting.retries",
        40,
        0,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> REPORT_WARNING_ENABLED_SETTING = Setting.boolSetting(
        "xpack.notification.reporting.warning.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting.AffixSetting<String> REPORT_WARNING_TEXT = Setting.affixKeySetting(
        "xpack.notification.reporting.warning.",
        "text",
        key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Dynamic)
    );

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("reporting_attachment");
    private static final ObjectParser<KibanaReportingPayload, Void> PAYLOAD_PARSER = new ObjectParser<>(
        "reporting_attachment_kibana_payload",
        true,
        null
    );

    static final Map<String, String> WARNINGS = Map.of(
        "kbn-csv-contains-formulas",
        "Warning: The attachment [%s] contains characters which spreadsheet applications may interpret as formulas. "
            + "Please ensure that the attachment is safe prior to opening."
    );

    static {
        PARSER.declareInt(Builder::retries, ReportingAttachment.RETRIES);
        PARSER.declareBoolean(Builder::inline, ReportingAttachment.INLINE);
        PARSER.declareString(Builder::interval, ReportingAttachment.INTERVAL);
        PARSER.declareString(Builder::url, ReportingAttachment.URL);
        PARSER.declareObjectOrDefault(Builder::auth, (p, s) -> {
            try {
                return BasicAuth.parse(p);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, () -> null, ReportingAttachment.AUTH);
        PARSER.declareObjectOrDefault(Builder::proxy, (p, s) -> {
            try {
                return HttpProxy.parse(p);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, () -> null, ReportingAttachment.PROXY);
        PAYLOAD_PARSER.declareString(KibanaReportingPayload::setPath, new ParseField("path"));
    }

    private static List<Setting<?>> getDynamicSettings() {
        return Arrays.asList(REPORT_WARNING_ENABLED_SETTING, REPORT_WARNING_TEXT);
    }

    private static List<Setting<?>> getStaticSettings() {
        return Arrays.asList(INTERVAL_SETTING, RETRIES_SETTING);
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> allSettings = new ArrayList<Setting<?>>(getDynamicSettings());
        allSettings.addAll(getStaticSettings());
        return allSettings;
    }

    private final Logger logger;
    private final TimeValue interval;
    private final int retries;
    private final WebhookService webhookService;
    private final TextTemplateEngine templateEngine;
    private boolean warningEnabled = REPORT_WARNING_ENABLED_SETTING.getDefault(Settings.EMPTY);
    private final Map<String, String> customWarnings = new ConcurrentHashMap<>(1);

    public ReportingAttachmentParser(
        Settings settings,
        WebhookService webhookService,
        TextTemplateEngine templateEngine,
        ClusterSettings clusterSettings
    ) {
        this.interval = INTERVAL_SETTING.get(settings);
        this.retries = RETRIES_SETTING.get(settings);
        this.webhookService = webhookService;
        this.templateEngine = templateEngine;
        this.logger = LogManager.getLogger(getClass());
        clusterSettings.addSettingsUpdateConsumer(REPORT_WARNING_ENABLED_SETTING, this::setWarningEnabled);
        clusterSettings.addAffixUpdateConsumer(REPORT_WARNING_TEXT, this::addWarningText, ReportingAttachmentParser::warningValidator);
    }

    void setWarningEnabled(boolean warningEnabled) {
        this.warningEnabled = warningEnabled;
    }

    void addWarningText(String name, String value) {
        customWarnings.put(name, value);
    }

    static void warningValidator(String name, String value) {
        if (WARNINGS.containsKey(name) == false) {
            throw new IllegalArgumentException(
                format(
                    "Warning [%s] is not supported. Only the following warnings are supported [%s]",
                    name,
                    String.join(", ", WARNINGS.keySet())
                )
            );
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public ReportingAttachment parse(String id, XContentParser parser) throws IOException {
        Builder builder = new Builder(id);
        PARSER.parse(parser, builder, null);
        return builder.build();
    }

    @Override
    public Attachment toAttachment(WatchExecutionContext context, Payload payload, ReportingAttachment attachment) throws IOException {
        Map<String, Object> model = Variables.createCtxParamsMap(context, payload);

        String initialUrl = templateEngine.render(new TextTemplate(attachment.url()), model);

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder(initialUrl)
            .connectionTimeout(TimeValue.timeValueSeconds(15))
            .readTimeout(TimeValue.timeValueSeconds(15))
            .method(HttpMethod.POST)
            .auth(attachment.auth())
            .proxy(attachment.proxy())
            .putHeader("kbn-xsrf", new TextTemplate("reporting"))
            .build();
        HttpRequest request = requestTemplate.render(templateEngine, model);

        HttpResponse reportGenerationResponse = requestReportGeneration(context.watch().id(), attachment.id(), request);
        String path = extractIdFromJson(context.watch().id(), attachment.id(), reportGenerationResponse.body());

        HttpRequestTemplate pollingRequestTemplate = HttpRequestTemplate.builder(request.host(), request.port())
            .connectionTimeout(TimeValue.timeValueSeconds(10))
            .readTimeout(TimeValue.timeValueSeconds(10))
            .auth(attachment.auth())
            .path(path)
            .scheme(request.scheme())
            .proxy(attachment.proxy())
            .putHeader("kbn-xsrf", new TextTemplate("reporting"))
            .build();
        HttpRequest pollingRequest = pollingRequestTemplate.render(templateEngine, model);

        int maxRetries = attachment.retries() != null ? attachment.retries() : this.retries;
        long sleepMillis = getSleepMillis(context, attachment);
        int retryCount = 0;
        while (retryCount < maxRetries) {
            retryCount++;
            // IMPORTANT NOTE: This is only a temporary solution until we made the execution of watcher more async
            // This still blocks other executions on the thread and we have to get away from that
            sleep(sleepMillis, context, attachment);
            HttpResponse response = webhookService.modifyAndExecuteHttpRequest(pollingRequest).v2();

            if (response.status() == 503) {
                // requires us to interval another run, no action to take, except logging
                logger.trace(
                    "Watch[{}] reporting[{}] pdf is not ready, polling in [{}] again",
                    context.watch().id(),
                    attachment.id(),
                    TimeValue.timeValueMillis(sleepMillis)
                );
            } else if (response.status() >= 400) {
                String body = response.body() != null ? response.body().utf8ToString() : null;
                throw new ElasticsearchException(
                    "Watch[{}] reporting[{}] Error when polling pdf from host[{}], port[{}], "
                        + "method[{}], path[{}], status[{}], body[{}]",
                    context.watch().id(),
                    attachment.id(),
                    request.host(),
                    request.port(),
                    request.method(),
                    request.path(),
                    response.status(),
                    body
                );
            } else if (response.status() == 200) {
                Set<String> warnings = Sets.newHashSetWithExpectedSize(1);
                if (warningEnabled) {
                    WARNINGS.forEach((warningKey, defaultWarning) -> {
                        String[] text = response.header(warningKey);
                        if (text != null && text.length > 0) {
                            if (Boolean.valueOf(text[0])) {
                                String warning = String.format(Locale.ROOT, defaultWarning, attachment.id());
                                String customWarning = customWarnings.get(warningKey);
                                if (Strings.isNullOrEmpty(customWarning) == false) {
                                    warning = String.format(Locale.ROOT, customWarning, attachment.id());
                                }
                                warnings.add(warning);
                            }
                        }
                    });
                }
                return new Attachment.Bytes(
                    attachment.id(),
                    attachment.id(),
                    BytesReference.toBytes(response.body()),
                    response.contentType(),
                    attachment.inline(),
                    warnings
                );
            } else {
                String body = response.body() != null ? response.body().utf8ToString() : null;
                String message = LoggerMessageFormat.format(
                    "",
                    "Watch[{}] reporting[{}] Unexpected status code host[{}], port[{}], " + "method[{}], path[{}], status[{}], body[{}]",
                    context.watch().id(),
                    attachment.id(),
                    request.host(),
                    request.port(),
                    request.method(),
                    request.path(),
                    response.status(),
                    body
                );
                throw new IllegalStateException(message);
            }
        }

        throw new ElasticsearchException(
            "Watch[{}] reporting[{}]: Aborting due to maximum number of retries hit [{}]",
            context.watch().id(),
            attachment.id(),
            maxRetries
        );
    }

    private static void sleep(long sleepMillis, WatchExecutionContext context, ReportingAttachment attachment) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ElasticsearchException(
                "Watch[{}] reporting[{}] thread was interrupted, while waiting for polling. Aborting.",
                context.watch().id(),
                attachment.id()
            );
        }
    }

    /**
     * Use the default time to sleep between polls if it was not set
     */
    private long getSleepMillis(WatchExecutionContext context, ReportingAttachment attachment) {
        long sleepMillis;
        if (attachment.interval() == null) {
            sleepMillis = interval.millis();
            logger.trace(
                "Watch[{}] reporting[{}] invalid interval configuration [{}], using configured default [{}]",
                context.watch().id(),
                attachment.id(),
                attachment.interval(),
                this.interval
            );
        } else {
            sleepMillis = attachment.interval().millis();
        }
        return sleepMillis;
    }

    /**
     * Trigger the initial report generation and catch possible exceptions
     */
    private HttpResponse requestReportGeneration(String watchId, String attachmentId, HttpRequest request) throws IOException {
        HttpResponse response = webhookService.modifyAndExecuteHttpRequest(request).v2();
        if (response.status() != 200) {
            throw new ElasticsearchException(
                "Watch[{}] reporting[{}] Error response when trying to trigger reporting generation "
                    + "host[{}], port[{}] method[{}], path[{}], response[{}]",
                watchId,
                attachmentId,
                request.host(),
                request.port(),
                request.method(),
                request.path(),
                response
            );
        }

        return response;
    }

    /**
     * Extract the id from JSON payload, so we know which ID to poll for
     */
    private static String extractIdFromJson(String watchId, String attachmentId, BytesReference body) throws IOException {
        // EMPTY is safe here becaus we never call namedObject
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                body,
                XContentType.JSON
            )
        ) {
            KibanaReportingPayload payload = new KibanaReportingPayload();
            PAYLOAD_PARSER.parse(parser, payload, null);
            String path = payload.getPath();
            if (Strings.isEmpty(path)) {
                throw new ElasticsearchException(
                    "Watch[{}] reporting[{}] field path found in JSON payload, payload was {}",
                    watchId,
                    attachmentId,
                    body.utf8ToString()
                );
            }
            return path;
        }
    }

    /**
     * Helper class to extract the URL path of the dashboard from the response after a report was triggered
     *
     * Example JSON: { "path" : "/path/to/dashboard.pdf", ... otherstuff ... }
     */
    static class KibanaReportingPayload {

        private String path;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    /**
     * Builder helper class used by the ObjectParser to create an attachment from xcontent input
     */
    static class Builder {

        private final String id;
        private boolean inline;
        private String url;
        private TimeValue interval;
        private Integer retries;
        private BasicAuth auth;
        private HttpProxy proxy;

        Builder(String id) {
            this.id = id;
        }

        Builder url(String url) {
            this.url = url;
            return this;
        }

        // package protected, so it can be used by the object parser in ReportingAttachmentParser
        Builder interval(String waitTime) {
            this.interval = TimeValue.parseTimeValue(waitTime, "attachment.reporting.interval");
            return this;
        }

        Builder retries(Integer retries) {
            this.retries = retries;
            return this;
        }

        Builder inline(boolean inline) {
            this.inline = inline;
            return this;
        }

        Builder auth(BasicAuth auth) {
            this.auth = auth;
            return this;
        }

        Builder proxy(HttpProxy proxy) {
            this.proxy = proxy;
            return this;
        }

        ReportingAttachment build() {
            return new ReportingAttachment(id, url, inline, interval, retries, auth, proxy);
        }
    }
}
