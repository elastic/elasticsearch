/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.webhook;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.ActionException;
import org.elasticsearch.alerts.support.StringTemplateUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class WebhookAction extends Action<WebhookAction.Result> {

    public static final String TYPE = "webhook";

    private final StringTemplateUtils templateUtils;
    private final HttpClient httpClient;

    private final StringTemplateUtils.Template urlTemplate;
    private final HttpMethod method;

    //Optional, default will be used if not provided
    private final StringTemplateUtils.Template bodyTemplate;

    private static final StringTemplateUtils.Template DEFAULT_BODY_TEMPLATE = new StringTemplateUtils.Template(
            "{ 'alertname' : '{{alert_name}}', 'request': {{request}}, 'response' : {{response}} }", null,
            "mustache", ScriptService.ScriptType.INLINE );

    protected WebhookAction(ESLogger logger, StringTemplateUtils templateUtils, HttpClient httpClient,
                            @Nullable StringTemplateUtils.Template bodyTemplate,
                            StringTemplateUtils.Template urlTemplate, HttpMethod method) {
        super(logger);
        this.templateUtils = templateUtils;
        this.httpClient = httpClient;
        this.bodyTemplate = bodyTemplate;
        this.urlTemplate = urlTemplate;
        this.method = method;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(Alert alert, Map<String, Object> data) throws IOException {
        String renderedUrl = applyTemplate(urlTemplate, alert, data);

        try {
            String body = applyTemplate(bodyTemplate != null ? bodyTemplate : DEFAULT_BODY_TEMPLATE, alert, data);
            int status = httpClient.execute(method, renderedUrl, body);
            if (status >= 400) {
                logger.warn("got status [" + status + "] when connecting to [" + renderedUrl + "]");
            } else {
                if (status >= 300) {
                    logger.warn("a 200 range return code was expected, but got [" + status + "]");
                }
            }
            return new Result(status < 400, status, renderedUrl, body);
        } catch (IOException ioe) {
            throw new ActionException("failed to connect to [" + renderedUrl + "] for alert [" + alert.name() + "]", ioe);
        }

    }

    String applyTemplate(StringTemplateUtils.Template template, Alert alert, Map<String, Object> data) {
        Map<String, Object> webHookParams = new HashMap<>();
        webHookParams.put(ALERT_NAME_VARIABLE_NAME, alert.name());
        webHookParams.put(RESPONSE_VARIABLE_NAME, data);
        return templateUtils.executeTemplate(template, webHookParams);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.METHOD_FIELD.getPreferredName(), method.getName());
        StringTemplateUtils.writeTemplate(Parser.BODY_TEMPLATE_FIELD.getPreferredName(), bodyTemplate, builder, params);
        StringTemplateUtils.writeTemplate(Parser.URL_TEMPLATE_FIELD.getPreferredName(), urlTemplate, builder, params);
        builder.endObject();
        return builder;
    }

    public static class Result extends Action.Result {

        private final int httpStatusCode;
        private final String url;
        private final String body;

        public Result(boolean success, int httpStatusCode, String url, String body) {
            super(TYPE, success);
            this.httpStatusCode = httpStatusCode;
            this.url = url;
            this.body = body;
        }

        public int httpStatusCode() {
            return httpStatusCode;
        }

        public String url() {
            return url;
        }

        public String body() {
            return body;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("success", success());
            builder.field("http_status", httpStatusCode());
            builder.field("url", url());
            builder.field("body", body());
            builder.endObject();
            return builder;
        }
    }


    public static class Parser extends AbstractComponent implements Action.Parser<WebhookAction> {

        public static final ParseField METHOD_FIELD = new ParseField("method");
        public static final ParseField URL_TEMPLATE_FIELD = new ParseField("url_template");
        public static final ParseField BODY_TEMPLATE_FIELD = new ParseField("body_template");

        private final StringTemplateUtils templateUtils;
        private final HttpClient httpClient;

        @Inject
        public Parser(Settings settings, StringTemplateUtils templateUtils, HttpClient httpClient) {
            super(settings);
            this.templateUtils = templateUtils;
            this.httpClient = httpClient;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public WebhookAction parse(XContentParser parser) throws IOException {
            HttpMethod method = HttpMethod.POST;
            StringTemplateUtils.Template urlTemplate = null;
            StringTemplateUtils.Template bodyTemplate = null;

            String currentFieldName = null;
            XContentParser.Token token;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (METHOD_FIELD.match(currentFieldName)) {
                        method = HttpMethod.valueOf(parser.text());
                        if (method != HttpMethod.POST && method != HttpMethod.GET && method != HttpMethod.PUT) {
                            throw new ActionException("could not parse webhook action. unsupported http method ["
                                    + method.getName() + "]");
                        }
                    } else if (URL_TEMPLATE_FIELD.match(currentFieldName)) {
                        urlTemplate = StringTemplateUtils.readTemplate(parser);
                    } else if (BODY_TEMPLATE_FIELD.match(currentFieldName)) {
                        bodyTemplate = StringTemplateUtils.readTemplate(parser);
                    } else {
                        throw new ActionException("could not parse webhook action. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionException("could not parse webhook action. unexpected token [" + token + "]");
                }
            }

            if (urlTemplate == null) {
                throw new ActionException("could not parse webhook action. [url_template] is required");
            }

            return new WebhookAction(logger, templateUtils, httpClient, bodyTemplate, urlTemplate, method);
        }
    }
}
