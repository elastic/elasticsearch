/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.ActionSettingsException;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.WatchExecutionContext;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class WebhookAction extends Action<WebhookAction.Result> {

    public static final String TYPE = "webhook";

    private final HttpClient httpClient;

    private final TemplatedHttpRequest templatedHttpRequest;

    public WebhookAction(ESLogger logger, @Nullable Transform transform, HttpClient httpClient, TemplatedHttpRequest templatedHttpRequest) {
        super(logger, transform);
        this.httpClient = httpClient;
        this.templatedHttpRequest = templatedHttpRequest;
    }

    public TemplatedHttpRequest templatedHttpRequest() {
        return templatedHttpRequest;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected Result execute(WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        HttpRequest httpRequest = templatedHttpRequest.render(model);
        try {
            try (HttpResponse response = httpClient.execute(httpRequest)) {
                int status = response.status();
                if (status >= 400) {
                    logger.warn("got status [" + status + "] when connecting to [" + httpRequest.host() + "] [" + httpRequest.path() + "]");
                } else {
                    if (status >= 300) {
                        logger.warn("a 200 range return code was expected, but got [" + status + "]");
                    }
                }
                return new Result.Executed(status, httpRequest, response.body());
            }
        } catch (IOException ioe) {
            logger.error("failed to connect to [{}] for watch [{}]", ioe, httpRequest.toString(), ctx.watch().name());
            return new Result.Failure("failed to send http request. " + ioe.getMessage());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (transform != null) {
            builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                    .field(transform.type(), transform)
                    .endObject();
        }
        builder.field(Parser.REQUEST_FIELD.getPreferredName(), templatedHttpRequest);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WebhookAction that = (WebhookAction) o;

        if (templatedHttpRequest != null ? !templatedHttpRequest.equals(that.templatedHttpRequest) : that.templatedHttpRequest != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return templatedHttpRequest != null ? templatedHttpRequest.hashCode() : 0;
    }

    public abstract static class Result extends Action.Result {

        public Result(String type, boolean success) {
            super(type, success);
        }

        void transformResult(Transform.Result result) {
            this.transformResult = result;
        }

        public static class Executed extends Result {

            private final int httpStatus;
            private final byte[] responseBody;
            private final HttpRequest httpRequest;

            public Executed(int httpStatus, HttpRequest httpRequest, byte[] responseBody) {
                super(TYPE, httpStatus < 400);
                this.httpStatus = httpStatus;
                this.responseBody = responseBody;
                this.httpRequest = httpRequest;
            }

            public int httpStatus() {
                return httpStatus;
            }

            public byte[] responseBody() {
                return responseBody;
            }

            public HttpRequest httpRequest() {
                return httpRequest;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(SUCCESS_FIELD.getPreferredName(), success())
                        .field(WebhookAction.Parser.HTTP_STATUS_FIELD.getPreferredName(), httpStatus)
                        .field(Parser.REQUEST_FIELD.getPreferredName(), httpRequest)
                        .field(Parser.RESPONSE_BODY.getPreferredName(), responseBody);
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
    }


    public static class Parser extends AbstractComponent implements Action.Parser<Result, WebhookAction> {

        public static final ParseField REQUEST_FIELD = new ParseField("request");
        public static final ParseField HTTP_STATUS_FIELD = new ParseField("http_status");
        public static final ParseField RESPONSE_BODY = new ParseField("response_body");
        public static final ParseField REASON_FIELD = new ParseField("reason");

        private final HttpClient httpClient;
        private final TransformRegistry transformRegistry;
        private final HttpRequest.Parser requestParser;
        private final TemplatedHttpRequest.Parser templatedRequestParser;


        @Inject
        public Parser(Settings settings, HttpClient httpClient,
                      TransformRegistry transformRegistry, HttpRequest.Parser requestParser,
                      TemplatedHttpRequest.Parser templatedRequestParser) {
            super(settings);
            this.httpClient = httpClient;
            this.transformRegistry = transformRegistry;
            this.requestParser = requestParser;
            this.templatedRequestParser = templatedRequestParser;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public WebhookAction parse(XContentParser parser) throws IOException {
            Transform transform = null;
            TemplatedHttpRequest templatedHttpRequest = null;

            String currentFieldName = null;
            XContentParser.Token token;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ((token == XContentParser.Token.START_OBJECT) && currentFieldName != null ) {
                    if (REQUEST_FIELD.match(currentFieldName)) {
                        templatedHttpRequest = templatedRequestParser.parse(parser);
                    } else if (Transform.Parser.TRANSFORM_FIELD.match(currentFieldName)) {
                        transform = transformRegistry.parse(parser);
                    }  else {
                        throw new ActionSettingsException("could not parse webhook action. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionSettingsException("could not parse webhook action. unexpected token [" + token + "]");
                }
            }

            if (templatedHttpRequest == null) {
                throw new ActionSettingsException("could not parse webhook action. [" + REQUEST_FIELD.getPreferredName() + "] is required");
            }

            return new WebhookAction(logger, transform, httpClient, templatedHttpRequest);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Transform.Result transformResult = null;
            String currentFieldName = null;
            XContentParser.Token token;
            Boolean success = null;
            String reason = null;
            HttpRequest request = null;
            byte[] responseBody = null;
            int httpStatus = -1;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (HTTP_STATUS_FIELD.match(currentFieldName)) {
                        httpStatus = parser.intValue();
                    } else if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        } else {
                            throw new ActionException("could not parse webhook result. unexpected boolean field [" + currentFieldName + "]");
                        }
                    } else if (RESPONSE_BODY.match(currentFieldName)) {
                        responseBody = parser.binaryValue();
                    }else {
                        throw new ActionException("unable to parse webhook action result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Transform.Parser.TRANSFORM_RESULT_FIELD.match(currentFieldName)) {
                        transformResult = transformRegistry.parseResult(parser);
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        request = requestParser.parse(parser);
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


            Result result = (reason == null) ? new Result.Executed(httpStatus, request, responseBody) : new Result.Failure(reason);
            if (transformResult != null) {
                result.transformResult(transformResult);
            }
            return result;
        }
    }

    private Object makeURLSafe(Object toSafe) throws UnsupportedEncodingException {
        if (toSafe instanceof List) {
            List<Object> returnObject = new ArrayList<>(((List) toSafe).size());
            for (Object o : (List)toSafe) {
                returnObject.add(makeURLSafe(o));
            }
            return returnObject;
        } else if (toSafe instanceof Map) {
            Map<Object, Object> returnObject = new HashMap<>(((Map) toSafe).size());
            for (Object key : ((Map) toSafe).keySet()) {
                returnObject.put(key, makeURLSafe(((Map) toSafe).get(key)));
            }
            return returnObject;
        } else if (toSafe instanceof String) {
            return URLEncoder.encode(toSafe.toString(), Charsets.UTF_8.name());
        } else {
            //Don't know how to convert anything else
            return toSafe;
        }
    }


    public static class SourceBuilder implements Action.SourceBuilder {

        private final TemplatedHttpRequest httpRequest;

        public SourceBuilder(TemplatedHttpRequest httpRequest) {
            this.httpRequest = httpRequest;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Parser.REQUEST_FIELD.getPreferredName(), httpRequest);
            return builder.endObject();
        }
    }
}
