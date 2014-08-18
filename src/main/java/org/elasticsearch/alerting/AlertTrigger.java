/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AlertTrigger implements ToXContent {

    private SimpleTrigger trigger;
    private TriggerType triggerType;
    private int value;

    public ScriptedAlertTrigger scriptedTrigger() {
        return scriptedTrigger;
    }

    public void scriptedTrigger(ScriptedAlertTrigger scriptedTrigger) {
        this.scriptedTrigger = scriptedTrigger;
    }

    private ScriptedAlertTrigger scriptedTrigger;

    public SimpleTrigger trigger() {
        return trigger;
    }

    public void trigger(SimpleTrigger trigger) {
        this.trigger = trigger;
    }

    public TriggerType triggerType() {
        return triggerType;
    }

    public void triggerType(TriggerType triggerType) {
        this.triggerType = triggerType;
    }

    public int value() {
        return value;
    }

    public void value(int value) {
        this.value = value;
    }

    public AlertTrigger(SimpleTrigger trigger, TriggerType triggerType, int value){
        this.trigger = trigger;
        this.triggerType = triggerType;
        this.value = value;
    }

    public AlertTrigger(ScriptedAlertTrigger scriptedTrigger){
        this.scriptedTrigger = scriptedTrigger;
        this.triggerType = TriggerType.SCRIPT;
    }

    public String toString(){
        if(triggerType != TriggerType.SCRIPT) {
            return triggerType + " " + trigger + " " + value;
        } else {
            return scriptedTrigger.toString();
        }
    }

    public static enum SimpleTrigger {
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        LESS_THAN,
        RISES_BY,
        FALLS_BY;

        public static SimpleTrigger fromString(final String sTrigger) {
            switch (sTrigger) {
                case ">":
                    return GREATER_THAN;
                case "<":
                    return LESS_THAN;
                case "=":
                case "==":
                    return EQUAL;
                case "!=":
                    return NOT_EQUAL;
                case "->":
                    return RISES_BY;
                case "<-":
                    return FALLS_BY;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown AlertAction:SimpleAction [" + sTrigger + "]");
            }
        }

        public static String asString(final SimpleTrigger trigger){
            switch (trigger) {
                case GREATER_THAN:
                    return ">";
                case LESS_THAN:
                    return "<";
                case EQUAL:
                    return "==";
                case NOT_EQUAL:
                    return "!=";
                case RISES_BY:
                    return "->";
                case FALLS_BY:
                    return "<-";
                default:
                    return "?";
            }
        }

        public String toString(){
            return asString(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (triggerType != TriggerType.SCRIPT) {
            builder.startObject();
            builder.field(triggerType.toString(), trigger.toString() + value);
            builder.endObject();
            return builder;
        } else {
            return scriptedTrigger.toXContent(builder, params);
        }
    }

    public static enum TriggerType {
        NUMBER_OF_EVENTS,
        SCRIPT;

        public static TriggerType fromString(final String sTriggerType) {
            switch (sTriggerType) {
                case "numberOfEvents":
                    return NUMBER_OF_EVENTS;
                case "script":
                    return SCRIPT;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown AlertTrigger:TriggerType [" + sTriggerType + "]");
            }
        }

        public static String asString(final TriggerType triggerType){
            switch (triggerType) {
                case NUMBER_OF_EVENTS:
                    return "numberOfEvents";
                case SCRIPT:
                    return "script";
                default:
                    return "unknown";
            }
        }

        public String toString(){
            return asString(this);
        }
    }

}
