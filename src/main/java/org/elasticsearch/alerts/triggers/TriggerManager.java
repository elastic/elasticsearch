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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TemplateQueryBuilder;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/*
 * TODO : The trigger classes need cleanup and refactoring to be similar to the AlertActions and be pluggable
 */
public class TriggerManager extends AbstractComponent {

    private final Client client;
    private final ScriptService scriptService;

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

    @Inject
    public TriggerManager(Settings settings, Client client, ScriptService scriptService) {
        super(settings);
        this.client = client;
        this.scriptService = scriptService;
    }

    public TriggerResult isTriggered(Alert alert, DateTime scheduledFireTime) throws Exception {
        SearchRequest request = createClampedRequest(scheduledFireTime, alert);
        if (logger.isTraceEnabled()) {
            logger.trace("For alert [{}] running query for [{}]", alert.alertName(), XContentHelper.convertToJson(request.source(), false, true));
        }

        SearchResponse response = client.search(request).get();
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
        return new TriggerResult(triggered, request, response);
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
        return new TriggerResult(triggered, request, response);
    }

    private SearchRequest createClampedRequest(DateTime scheduledFireTime, Alert alert){
        DateTime clampEnd = new DateTime(scheduledFireTime);
        DateTime clampStart = clampEnd.minusSeconds((int)alert.timePeriod().seconds());
        SearchRequest request = new SearchRequest(alert.indices().toArray(new String[0]));
        if (alert.simpleQuery()) {
            TemplateQueryBuilder queryBuilder = new TemplateQueryBuilder(alert.queryName(), ScriptService.ScriptType.INDEXED, new HashMap<String, Object>());
            RangeFilterBuilder filterBuilder = new RangeFilterBuilder(alert.timestampString());
            filterBuilder.gte(clampStart);
            filterBuilder.lt(clampEnd);
            request.source(new SearchSourceBuilder().query(new FilteredQueryBuilder(queryBuilder, filterBuilder)));
        } else {
            //We can't just wrap the template here since it probably contains aggs or something else that doesn't play nice with FilteredQuery
            Map<String,Object> fromToMap = new HashMap<>();
            fromToMap.put("from", clampStart); //@TODO : make these parameters configurable ? Don't want to bloat the API too much tho
            fromToMap.put("to", clampEnd);
            //Go and get the search template from the script service :(
            ExecutableScript script =  scriptService.executable("mustache", alert.queryName(), ScriptService.ScriptType.INDEXED, fromToMap);
            BytesReference requestBytes = (BytesReference)(script.run());
            request.source(requestBytes, false);
        }
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
}
