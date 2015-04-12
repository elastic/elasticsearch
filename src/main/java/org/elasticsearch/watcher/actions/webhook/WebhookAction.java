/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Map;

/**
 */
public class WebhookAction extends Action<WebhookAction.Result> {

    public static final String TYPE = "webhook";

    private final HttpClient httpClient;

    private final HttpRequestTemplate requestTemplate;
    private final TemplateEngine templateEngine;

    public WebhookAction(ESLogger logger, HttpClient httpClient, HttpRequestTemplate requestTemplate, TemplateEngine templateEngine) {
        super(logger);
        this.httpClient = httpClient;
        this.requestTemplate = requestTemplate;
        this.templateEngine = templateEngine;
    }

    public HttpRequestTemplate requestTemplate() {
        return requestTemplate;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        HttpRequest request = requestTemplate.render(templateEngine, model);
        try {

            if (ctx.simulateAction(actionId)) {
                return new Result.Simulated(request);
            }
            HttpResponse response = httpClient.execute(request);

            int status = response.status();
            if (status >= 300) {
                logger.warn("received http status [{}] when connecting to [{}] [{}]", status, request.host(), request.path());
            }
            return new Result.Executed(request, response);
        } catch (IOException ioe) {
            logger.error("failed to execute webhook action [{}]. could not connect to [{}]", ioe, actionId, ctx.watch().name(), request.toString());
            return new Result.Failure("failed to send http request. " + ioe.getMessage());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return requestTemplate.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WebhookAction that = (WebhookAction) o;

        if (requestTemplate != null ? !requestTemplate.equals(that.requestTemplate) : that.requestTemplate != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return requestTemplate != null ? requestTemplate.hashCode() : 0;
    }

    public abstract static class Result extends Action.Result {

        public Result(String type, boolean success) {
            super(type, success);
        }

        public static class Executed extends Result {

            private final HttpRequest request;
            private final HttpResponse response;

            public Executed(HttpRequest request, HttpResponse response) {
                super(TYPE, response.status() < 400);
                this.request = request;
                this.response = response;
            }

            public HttpResponse response() {
                return response;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.REQUEST_FIELD.getPreferredName(), request)
                        .field(Parser.RESPONSE_FIELD.getPreferredName(), response);
            }
        }

        public static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(TYPE, false);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(WebhookAction.Parser.REASON_FIELD.getPreferredName(), reason);
            }
        }

        public static class Simulated extends Result {
            private final HttpRequest request;

            public Simulated(HttpRequest request) {
                super(TYPE, true);
                this.request = request;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.SIMULATED_REQUEST_FIELD.getPreferredName(), request);
            }
        }
    }


    public static class Parser extends AbstractComponent implements Action.Parser<Result, WebhookAction> {

        public static final ParseField REQUEST_FIELD = new ParseField("request");
        public static final ParseField SIMULATED_REQUEST_FIELD = new ParseField("simulated_request");
        public static final ParseField RESPONSE_FIELD = new ParseField("response");
        public static final ParseField REASON_FIELD = new ParseField("reason");

        private final HttpClient httpClient;
        private final HttpRequest.Parser requestParser;
        private final HttpRequestTemplate.Parser requestTemplateParser;
        private final TemplateEngine templateEngine;
        private final ESLogger actionLogger;

        @Inject
        public Parser(Settings settings, HttpClient httpClient, HttpRequest.Parser requestParser,
                      HttpRequestTemplate.Parser requestTemplateParser, TemplateEngine templateEngine) {
            super(settings);
            this.httpClient = httpClient;
            this.requestParser = requestParser;
            this.requestTemplateParser = requestTemplateParser;
            this.templateEngine = templateEngine;
            this.actionLogger = Loggers.getLogger(WebhookAction.class, settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public WebhookAction parse(XContentParser parser) throws IOException {
            try {
                HttpRequestTemplate request = requestTemplateParser.parse(parser);
                return new WebhookAction(actionLogger, httpClient, request, templateEngine);
            } catch (HttpRequestTemplate.ParseException pe) {
                throw new ActionException("could not parse webhook action", pe);
            }
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Boolean success = null;
            String reason = null;
            HttpRequest request = null;
            HttpRequest simulatedRequest = null;
            HttpResponse response = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (REQUEST_FIELD.match(currentFieldName)) {
                        request = requestParser.parse(parser);
                    } else if (SIMULATED_REQUEST_FIELD.match(currentFieldName)) {
                        simulatedRequest = requestParser.parse(parser);
                    } else if (RESPONSE_FIELD.match(currentFieldName)) {
                        response = HttpResponse.parse(parser);
                    } else {
                        throw new ActionException("unable to parse webhook action result. unexpected object field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new ActionException("unable to parse webhook action result. unexpected string field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                        success = parser.booleanValue();
                    } else {
                        throw new ActionException("unable to parse webhook action result. unexpected boolean field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionException("unable to parse webhook action result. unexpected token [" + token + "]" );
                }
            }

            if (success == null) {
                throw new ActionException("could not parse webhook result. expected boolean field [success]");
            }

            if (simulatedRequest != null) {
                return new Result.Simulated(simulatedRequest);
            }

            return (reason == null) ? new Result.Executed(request, response) : new Result.Failure(reason);
        }
    }

    public static class SourceBuilder extends Action.SourceBuilder<SourceBuilder> {

        private final HttpRequestTemplate requestTemplate;

        public SourceBuilder(HttpRequestTemplate requestTemplate) {
            this.requestTemplate = requestTemplate;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder actionXContent(XContentBuilder builder, Params params) throws IOException {
            return requestTemplate.toXContent(builder, params);

        }
    }
}
