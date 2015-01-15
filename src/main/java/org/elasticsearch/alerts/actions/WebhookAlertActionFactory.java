/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * This action factory will call back a web hook using a templatized url
 * If the method is PUT or POST the request,response and alert name will be sent as well
 */
public class WebhookAlertActionFactory implements AlertActionFactory {
    private static String ALERT_NAME = "alert_name";
    private static String RESPONSE = "response";
    private static String REQUEST = "request";
    static String DEFAULT_PARAMETER_STRING = "alertname={{alert_name}}&request=%{{request}}&response=%{{response}}";

    private final ScriptService scriptService;

    private final Logger logger = Logger.getLogger(WebhookAlertActionFactory.class);

    public WebhookAlertActionFactory(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    @Override
    public AlertAction createAction(XContentParser parser) throws IOException {
        String url = null;
        HttpMethod method = HttpMethod.POST;
        String parameterTemplate = DEFAULT_PARAMETER_STRING;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "url":
                        url = parser.text();
                        break;
                    case "method":
                        method = HttpMethod.valueOf(parser.text());
                        if (method != HttpMethod.POST && method != HttpMethod.GET && method != HttpMethod.PUT) {
                            throw new ElasticsearchIllegalArgumentException("Unsupported http method ["
                                    + method.getName() + "]");
                        }
                        break;
                    case "parameter_string":
                        parameterTemplate = parser.text();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
            }
        }
        return new WebhookAlertAction(url, method, parameterTemplate);
    }

    @Override
    public boolean doAction(AlertAction action, Alert alert, TriggerResult result) {
        if (!(action instanceof WebhookAlertAction)) {
            throw new ElasticsearchIllegalStateException("Bad action [" + action.getClass() + "] passed to WebhookAlertActionFactory expected [" + WebhookAlertAction.class + "]");
        }
        WebhookAlertAction webhookAlertAction = (WebhookAlertAction)action;

        String url = webhookAlertAction.getUrl();
        String renderedUrl = renderUrl(url, alert, result, scriptService);

        HttpMethod method = webhookAlertAction.getMethod();

        try {
            URL urlToOpen = new URL(renderedUrl);

            HttpURLConnection httpConnection = (HttpURLConnection) urlToOpen.openConnection();
            httpConnection.setRequestMethod(method.getName());
            httpConnection.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());

            if (method == HttpMethod.POST || method == HttpMethod.PUT) {
                httpConnection.setDoOutput(true);
                httpConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset="
                        + StandardCharsets.UTF_8.name());

                String parameters = encodeParameterString(webhookAlertAction.getParameterString(), alert, result, scriptService);

                httpConnection.getOutputStream().write(parameters.getBytes(StandardCharsets.UTF_8.name()));
            }

            int status = httpConnection.getResponseCode();

            if (status >= 400) {
                throw new ElasticsearchException("Got status [" + status + "] when connecting to [" + renderedUrl + "]");
            } else {
                if (status >= 300) {
                    logger.warn("A 200 range return code was expected, but got [" + status + "]");
                }
                return true;
            }

        }  catch (IOException ioe) {
            throw new ElasticsearchException("Unable to connect to [" + renderedUrl + "]", ioe);

        }

    }

    static String encodeParameterString(String parameterString, Alert alert, TriggerResult result, ScriptService scriptService) throws IOException {
        XContentBuilder responseBuilder = XContentFactory.jsonBuilder();

        responseBuilder.startObject();
        responseBuilder.field("response",result.getTriggerResponse());
        responseBuilder.endObject();

        String requestJSON = XContentHelper.convertToJson(result.getTriggerRequest().source(), true);
        String responseJSON = XContentHelper.convertToJson(responseBuilder.bytes(), true);

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put(ALERT_NAME, URLEncoder.encode(alert.getAlertName(), StandardCharsets.UTF_8.name()));
        templateParams.put(RESPONSE, URLEncoder.encode(responseJSON, StandardCharsets.UTF_8.name()));
        templateParams.put(REQUEST, URLEncoder.encode(requestJSON, StandardCharsets.UTF_8.name()));

        ExecutableScript script = scriptService.executable("mustache", parameterString, ScriptService.ScriptType.INLINE, templateParams);
        return ((BytesReference) script.run()).toUtf8();

    }


    public String renderUrl(String url, Alert alert, TriggerResult result, ScriptService scriptService) {
         Map<String, Object> templateParams = new HashMap<>();
         templateParams.put(ALERT_NAME, alert.getAlertName());
         templateParams.put(RESPONSE, result.getActionResponse());
         ExecutableScript script = scriptService.executable("mustache", url, ScriptService.ScriptType.INLINE, templateParams);
         return ((BytesReference) script.run()).toUtf8();
     }
}
