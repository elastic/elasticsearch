/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;


/*
 * TODO : The trigger classes need cleanup and refactoring to be similar to the AlertActions and be pluggable
 */
public class TriggerManager extends AbstractComponent {

    private static final FormatDateTimeFormatter dateTimeFormatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;

    private final Client client;
    private final ScriptService scriptService;
    private final String fireTimePlaceHolder;
    private final String scheduledFireTimePlaceHolder;

    @Inject
    public TriggerManager(Settings settings, Client client, ScriptService scriptService) {
        super(settings);
        this.client = client;
        this.scriptService = scriptService;
        this.fireTimePlaceHolder = settings.get("prefix", "<<<FIRE_TIME>>>");
        this.scheduledFireTimePlaceHolder = settings.get("postfix", "<<<SCHEDULED_FIRE_TIME>>>");
    }

    public TriggerResult isTriggered(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        SearchRequest request = prepareTriggerSearch(alert, scheduledFireTime, fireTime);
        if (logger.isTraceEnabled()) {
            logger.trace("For alert [{}] running query for [{}]", alert.alertName(), XContentHelper.convertToJson(request.source(), false, true));
        }

        SearchResponse response = client.search(request).actionGet(); // actionGet deals properly with InterruptedException
        logger.debug("Ran alert [{}] and got hits : [{}]", alert.alertName(), response.getHits().getTotalHits());
        switch (alert.trigger().triggerType()) {
            case NUMBER_OF_EVENTS:
                return doSimpleTrigger(alert, request, response);
            case SCRIPT:
                return doScriptTrigger(alert, request, response);
            default:
                throw new ElasticsearchIllegalArgumentException("Bad value for trigger.triggerType [" + alert.trigger().triggerType() + "]");
        }
    }

    public static AlertTrigger parseTrigger(XContentParser parser) throws IOException {
        AlertTrigger trigger = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                    case "script":
                        String script = null;
                        ScriptService.ScriptType scriptType = null;
                        String scriptLang = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                switch (currentFieldName) {
                                    case "script" :
                                        script = parser.text();
                                        break;
                                    case "script_type" :
                                        scriptType = ScriptService.ScriptType.valueOf(parser.text());
                                        break;
                                    case "script_lang" :
                                        scriptLang = parser.text();
                                        break;
                                    default:
                                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                                }
                            }
                        }
                        trigger = new AlertTrigger(new ScriptedAlertTrigger(script, scriptType, scriptLang));
                        break;
                    default:
                        break;
                }
            } else if (token.isValue()) {
                String expression = parser.text();
                AlertTrigger.SimpleTrigger simpleTrigger = AlertTrigger.SimpleTrigger.fromString(expression.substring(0, 1));
                int value = Integer.valueOf(expression.substring(1));
                trigger = new AlertTrigger(simpleTrigger, AlertTrigger.TriggerType.NUMBER_OF_EVENTS, value);
            }
        }
        return trigger;
    }

    private TriggerResult doSimpleTrigger(Alert alert, SearchRequest request, SearchResponse response) {
        boolean triggered = false;
        long testValue = response.getHits().getTotalHits();
        int triggerValue = alert.trigger().value();
        //Move this to SimpleTrigger
        switch (alert.trigger().trigger()) {
            case GREATER_THAN:
                triggered = testValue > triggerValue;
                break;
            case LESS_THAN:
                triggered = testValue < triggerValue;
                break;
            case EQUAL:
                triggered = testValue == triggerValue;
                break;
            case NOT_EQUAL:
                triggered = testValue != triggerValue;
                break;
            case RISES_BY:
            case FALLS_BY:
                triggered = false; //TODO FIX THESE
                break;
        }
        return new TriggerResult(triggered, request, response, alert.trigger());
    }

    private TriggerResult doScriptTrigger(Alert alert, SearchRequest request, SearchResponse response) {
        boolean triggered = false;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            Map<String, Object> responseMap = XContentHelper.convertToMap(builder.bytes(), false).v2();

            ScriptedAlertTrigger scriptTrigger = alert.trigger().scriptedTrigger();
            ExecutableScript executable = scriptService.executable(
                    scriptTrigger.scriptLang, scriptTrigger.script, scriptTrigger.scriptType, responseMap
            );

            Object returnValue = executable.run();
            logger.trace("Returned [{}] from script", returnValue);
            if (returnValue instanceof Boolean) {
                triggered = (Boolean) returnValue;
            } else {
                throw new ElasticsearchIllegalStateException("Trigger script [" + scriptTrigger.script + "] did not return a Boolean");
            }
        } catch (Exception e ){
            logger.error("Failed to execute script trigger", e);
        }
        return new TriggerResult(triggered, request, response, alert.trigger());
    }

    private SearchRequest prepareTriggerSearch(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        SearchRequest request = alert.getSearchRequest();
        if (Strings.hasLength(request.source())) {
            String requestSource = XContentHelper.convertToJson(request.source(), false);
            if (requestSource.contains(fireTimePlaceHolder)) {
                requestSource = requestSource.replace(fireTimePlaceHolder, dateTimeFormatter.printer().print(fireTime));
            }
            if (requestSource.contains(scheduledFireTimePlaceHolder)) {
                requestSource = requestSource.replace(scheduledFireTimePlaceHolder, dateTimeFormatter.printer().print(scheduledFireTime));
            }
            request.source(requestSource);
        } else if (Strings.hasLength(request.templateSource())) {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(request.templateSource(), false);
            Map<String, Object> templateSourceAsMap = tuple.v2();
            Map<String, Object> templateObject = (Map<String, Object>) templateSourceAsMap.get("template");
            if (templateObject != null) {
                Map<String, Object> params = (Map<String, Object>) templateObject.get("params");
                params.put("scheduled_fire_time", dateTimeFormatter.printer().print(scheduledFireTime));
                params.put("fire_time", dateTimeFormatter.printer().print(fireTime));

                XContentBuilder builder = XContentFactory.contentBuilder(tuple.v1());
                builder.map(templateSourceAsMap);
                request.templateSource(builder.bytes(), false);
            }
        } else if (request.templateName() != null) {
            MapBuilder<String, String> templateParams = MapBuilder.newMapBuilder(request.templateParams())
                    .put("scheduled_fire_time", dateTimeFormatter.printer().print(scheduledFireTime))
                    .put("fire_time", dateTimeFormatter.printer().print(fireTime));
            request.templateParams(templateParams.map());
        } else {
            throw new ElasticsearchIllegalStateException("Search requests needs either source, template source or template name");
        }
        return request;
    }
}
