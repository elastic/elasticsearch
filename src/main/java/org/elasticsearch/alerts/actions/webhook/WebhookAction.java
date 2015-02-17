/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.webhook;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class WebhookAction extends Action<WebhookAction.Result> {

    public static final String TYPE = "webhook";

    static final StringTemplateUtils.Template DEFAULT_BODY_TEMPLATE = new StringTemplateUtils.Template(
            "{ 'alertname' : '{{alert_name}}', 'response' : {{response}} }");

    private final StringTemplateUtils templateUtils;
    private final HttpClient httpClient;

    private final StringTemplateUtils.Template urlTemplate;
    private final HttpMethod method;

    //Optional, default will be used if not provided
    private final StringTemplateUtils.Template bodyTemplate;

    public WebhookAction(ESLogger logger, StringTemplateUtils templateUtils, HttpClient httpClient,
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
    public Result execute(ExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> data = payload.data();
        String renderedUrl = applyTemplate(templateUtils, urlTemplate, ctx.alert().name(), data);
        String body = applyTemplate(templateUtils, bodyTemplate != null ? bodyTemplate : DEFAULT_BODY_TEMPLATE, ctx.alert().name(), data);
        try {

            int status = httpClient.execute(method, renderedUrl, body);
            if (status >= 400) {
                logger.warn("got status [" + status + "] when connecting to [" + renderedUrl + "]");
            } else {
                if (status >= 300) {
                    logger.warn("a 200 range return code was expected, but got [" + status + "]");
                }
            }
            return new Result.Executed(status, renderedUrl, body);
        } catch (IOException ioe) {
            logger.error("failed to connect to [{}] for alert [{}]", ioe, renderedUrl, ctx.alert().name());
            return new Result.Failure("failed to send http request. " + ioe.getMessage());
        }

    }

    static String applyTemplate(StringTemplateUtils templateUtils, StringTemplateUtils.Template template, String alertName, Map<String, Object> data) {
        Map<String, Object> webHookParams = new HashMap<>();
        webHookParams.put(ALERT_NAME_VARIABLE_NAME, alertName);
        webHookParams.put(RESPONSE_VARIABLE_NAME, data);
        return templateUtils.executeTemplate(template, webHookParams);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.METHOD_FIELD.getPreferredName(), method.getName());
        if (bodyTemplate != null) {
            StringTemplateUtils.writeTemplate(Parser.BODY_TEMPLATE_FIELD.getPreferredName(), bodyTemplate, builder, params);
        }
        StringTemplateUtils.writeTemplate(Parser.URL_TEMPLATE_FIELD.getPreferredName(), urlTemplate, builder, params);
        builder.endObject();
        return builder;
    }

    public abstract static class Result extends Action.Result {

        public Result(String type, boolean success) {
            super(type, success);
        }

        public static class Executed extends Result {

            private final int httpStatusCode;
            private final String url;
            private final String body;

            public Executed(int httpStatusCode, String url, String body) {
                super(TYPE, httpStatusCode < 400);
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field("success", success())
                        .field(Parser.HTTP_STATUS_FIELD.getPreferredName(), httpStatusCode)
                        .field(Parser.URL_FIELD.getPreferredName(), url)
                        .field(Parser.BODY_FIELD.getPreferredName(), body);
            }
        }

        public static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(TYPE, false);
                this.reason = reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.REASON_FIELD.getPreferredName(), reason);
            }
        }
    }


    public static class Parser extends AbstractComponent implements Action.Parser<Result, WebhookAction> {

        public static final ParseField METHOD_FIELD = new ParseField("method");
        public static final ParseField URL_TEMPLATE_FIELD = new ParseField("url_template");
        public static final ParseField BODY_TEMPLATE_FIELD = new ParseField("body_template");

        public static final ParseField BODY_FIELD = new ParseField("body");
        public static final ParseField URL_FIELD = new ParseField("url");
        public static final ParseField HTTP_STATUS_FIELD = new ParseField("http_status");
        public static final ParseField REASON_FIELD = new ParseField("reason");

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
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT) && currentFieldName != null ) {
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
                    }  else {
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

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            String currentFieldName = null;
            XContentParser.Token token;
            Boolean success = null;
            String url = null;
            String body = null;
            String reason = null;
            int httpStatus = -1;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (URL_FIELD.match(currentFieldName)) {
                        url = parser.text();
                    } else if (BODY_FIELD.match(currentFieldName)) {
                        body = parser.text();
                    } else if (HTTP_STATUS_FIELD.match(currentFieldName)) {
                        httpStatus = parser.intValue();
                    } else if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        } else {
                            throw new ActionException("could not parse webhook result. unexpected boolean field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ActionException("unable to parse webhook action result. unexpected field [" + currentFieldName + "]" );
                    }
                } else {
                    throw new ActionException("unable to parse webhook action result. unexpected field [" + currentFieldName + "]" );
                }
            }

            if (success == null) {
                throw new ActionException("could not parse webhook result. expected boolean field [success]");
            }

            return success ? new Result.Executed(httpStatus, url, body) : new Result.Failure(reason);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WebhookAction that = (WebhookAction) o;

        if (bodyTemplate != null ? !bodyTemplate.equals(that.bodyTemplate) : that.bodyTemplate != null) return false;
        if (method != null ? !method.equals(that.method) : that.method != null) return false;
        if (urlTemplate != null ? !urlTemplate.equals(that.urlTemplate) : that.urlTemplate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = urlTemplate != null ? urlTemplate.hashCode() : 0;
        result = 31 * result + (method != null ? method.hashCode() : 0);
        result = 31 * result + (bodyTemplate != null ? bodyTemplate.hashCode() : 0);
        return result;
    }
}
