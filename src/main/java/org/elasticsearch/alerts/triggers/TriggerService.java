/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TriggerService extends AbstractComponent {

    private final ClientProxy client;
    private final ScriptServiceProxy scriptService;
    private volatile ImmutableOpenMap<String, TriggerFactory> triggersImplemented;

    @Inject
    public TriggerService(Settings settings, ClientProxy client, ScriptServiceProxy scriptService) {
        super(settings);
        this.client = client;
        this.scriptService = scriptService;
        triggersImplemented = ImmutableOpenMap.<String, TriggerFactory>builder()
                .fPut("script", new ScriptedTriggerFactory(scriptService))
                .build();
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
     * @return a new AlertTrigger instance from the parser
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
        SearchRequest request = AlertUtils.createSearchRequestWithTimes(alert.getTriggerSearchRequest(), scheduledFireTime, fireTime, scriptService);
        if (logger.isTraceEnabled()) {
            logger.trace("For alert [{}] running query for [{}]", alert.getAlertName(), XContentHelper.convertToJson(request.source(), false, true));
        }

        SearchResponse response = client.search(request).actionGet(); // actionGet deals properly with InterruptedException
        if (logger.isDebugEnabled()) {
            logger.debug("Ran alert [{}] and got hits : [{}]", alert.getAlertName(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("Hit: {}", XContentHelper.toString(hit));
            }

        }
        XContentBuilder builder = jsonBuilder().startObject().value(response).endObject();
        Map<String, Object> responseMap = XContentHelper.convertToMap(builder.bytes(), false).v2();
        return isTriggered(alert.getTrigger(), request, responseMap);
    }

    protected TriggerResult isTriggered(AlertTrigger trigger, SearchRequest request, Map<String, Object> response) {
        TriggerFactory factory = triggersImplemented.get(trigger.getTriggerName());
        if (factory == null) {
            throw new ElasticsearchIllegalArgumentException("No trigger exists with the name [" + trigger.getTriggerName() + "]");
        }

        boolean triggered = factory.isTriggered(trigger, request,response);
        return new TriggerResult(triggered, request, response, trigger);
    }

}
