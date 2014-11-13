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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

public class TriggerManager extends AbstractComponent {


    private static final FormatDateTimeFormatter dateTimeFormatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;

    private volatile ImmutableOpenMap<String, TriggerFactory> triggersImplemented;
    private final Client client;
    private final String fireTimePlaceHolder;
    private final String scheduledFireTimePlaceHolder;


    @Inject
    public TriggerManager(Settings settings, Client client, ScriptService scriptService) {
        super(settings);
        this.client = client;
        triggersImplemented = ImmutableOpenMap.<String, TriggerFactory>builder()
                .fPut("script", new ScriptedTriggerFactory(scriptService))
                .build();
        this.fireTimePlaceHolder = settings.get("prefix", "{{FIRE_TIME}}");
        this.scheduledFireTimePlaceHolder = settings.get("postfix", "{{SCHEDULED_FIRE_TIME}}");
    }

    /**
     * Registers an AlertTrigger so that it can be instantiated by name
     * @param name The name of the trigger
     * @param actionFactory The factory responsible for trigger instantiation
     */
    public void registerTrigger(String name, TriggerFactory actionFactory){
        triggersImplemented = ImmutableOpenMap.builder(triggersImplemented)
                .fPut(name, actionFactory)
                .build();
    }


    /**
     * Reads the contents of parser to create the correct Trigger
     * @param parser The parser containing the trigger definition
     * @return
     * @throws IOException
     */
    public AlertTrigger instantiateAlertTrigger(XContentParser parser) throws IOException {
        ImmutableOpenMap<String, TriggerFactory> triggersImplemented = this.triggersImplemented;
        String triggerFactoryName = null;
        XContentParser.Token token;
        AlertTrigger trigger = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                triggerFactoryName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && triggerFactoryName != null) {
                TriggerFactory factory = triggersImplemented.get(triggerFactoryName);
                if (factory != null) {
                    trigger = factory.createTrigger(parser);
                } else {
                    throw new ElasticsearchIllegalArgumentException("No trigger exists with the name [" + triggerFactoryName + "]");
                }
            }
        }
        return trigger;
    }

    /**
     * Tests to see if an alert will trigger for a given fireTime and scheduleFire time
     *
     * @param alert The Alert to test
     * @param scheduledFireTime The time the alert was scheduled to run
     * @param fireTime The time the alert actually ran
     * @return The TriggerResult representing the trigger state of the alert
     * @throws IOException
     */
    public TriggerResult isTriggered(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        SearchRequest request = prepareTriggerSearch(alert, scheduledFireTime, fireTime);
        if (logger.isTraceEnabled()) {
            logger.trace("For alert [{}] running query for [{}]", alert.alertName(), XContentHelper.convertToJson(request.source(), false, true));
        }

        SearchResponse response = client.search(request).actionGet(); // actionGet deals properly with InterruptedException
        logger.debug("Ran alert [{}] and got hits : [{}]", alert.alertName(), response.getHits().getTotalHits());
        return isTriggered(alert.trigger(), request, response);
    }

    protected TriggerResult isTriggered(AlertTrigger trigger, SearchRequest request, SearchResponse response) {
        TriggerFactory factory = triggersImplemented.get(trigger.getTriggerName());

        if (factory == null) {
            throw new ElasticsearchIllegalArgumentException("No trigger exists with the name [" + trigger.getTriggerName() + "]");
        }

        boolean triggered = factory.isTriggered(trigger, request,response);
        return new TriggerResult(triggered, request, response, trigger);
    }

    private SearchRequest prepareTriggerSearch(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        SearchRequest triggerSearchRequest = new SearchRequest(alert.getSearchRequest())
                .indicesOptions(alert.getSearchRequest().indicesOptions())
                .indices(alert.getSearchRequest().indices());
        if (Strings.hasLength(alert.getSearchRequest().source())) {
            String requestSource = XContentHelper.convertToJson(alert.getSearchRequest().source(), false);
            if (requestSource.contains(fireTimePlaceHolder)) {
                requestSource = requestSource.replace(fireTimePlaceHolder, dateTimeFormatter.printer().print(fireTime));
            }
            if (requestSource.contains(scheduledFireTimePlaceHolder)) {
                requestSource = requestSource.replace(scheduledFireTimePlaceHolder, dateTimeFormatter.printer().print(scheduledFireTime));
            }

            triggerSearchRequest.source(requestSource);
        } else if (Strings.hasLength(alert.getSearchRequest().templateSource())) {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(alert.getSearchRequest().templateSource(), false);
            Map<String, Object> templateSourceAsMap = tuple.v2();
            Map<String, Object> templateObject = (Map<String, Object>) templateSourceAsMap.get("template");
            if (templateObject != null) {
                Map<String, Object> params = (Map<String, Object>) templateObject.get("params");
                params.put("scheduled_fire_time", dateTimeFormatter.printer().print(scheduledFireTime));
                params.put("fire_time", dateTimeFormatter.printer().print(fireTime));

                XContentBuilder builder = XContentFactory.contentBuilder(tuple.v1());
                builder.map(templateSourceAsMap);
                triggerSearchRequest.templateSource(builder.bytes(), false);
            }
        } else if (alert.getSearchRequest().templateName() != null) {
            MapBuilder<String, String> templateParams = MapBuilder.newMapBuilder(alert.getSearchRequest().templateParams())
                    .put("scheduled_fire_time", dateTimeFormatter.printer().print(scheduledFireTime))
                    .put("fire_time", dateTimeFormatter.printer().print(fireTime));
            triggerSearchRequest.templateParams(templateParams.map());
        } else {
            throw new ElasticsearchIllegalStateException("Search requests needs either source, template source or template name");
        }
        return triggerSearchRequest;
    }


}
