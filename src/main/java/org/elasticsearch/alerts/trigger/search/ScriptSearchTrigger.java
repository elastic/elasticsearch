/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.alerts.trigger.TriggerException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

/**
 *
 */
public class ScriptSearchTrigger extends SearchTrigger {

    public static final String TYPE = "script";

    private final String script;
    private final ScriptService.ScriptType scriptType;
    private final String scriptLang;

    public ScriptSearchTrigger(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client,
                               SearchRequest request, String script, ScriptService.ScriptType scriptType,
                               String scriptLang) {
        super(logger, scriptService, client, request);
        this.script = script;
        this.scriptType = scriptType;
        this.scriptLang = scriptLang;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected Result processSearchResponse(SearchResponse response) {
        Payload payload = new Payload.ActionResponse(response);
        ExecutableScript executable = scriptService.executable(scriptLang, script, scriptType, payload.data());
        Object value = executable.run();
        if (value instanceof Boolean) {
            return new Result(TYPE, (Boolean) value, request, payload);
        }
        throw new TriggerException("trigger script [" + script + "] did not return a boolean value");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.REQUEST_FIELD.getPreferredName());
        AlertUtils.writeSearchRequest(request, builder, params);
        builder.field(ScriptService.SCRIPT_INLINE.getPreferredName(), script);
        builder.field(Parser.SCRIPT_TYPE_FIELD.getPreferredName(), scriptType);
        builder.field(ScriptService.SCRIPT_LANG.getPreferredName(), scriptLang);
        builder.endObject();
        return builder;
    }

    public static class Parser extends AbstractComponent implements SearchTrigger.Parser<ScriptSearchTrigger> {

        public static ParseField REQUEST_FIELD = new ParseField("request");
        public static ParseField SCRIPT_TYPE_FIELD = new ParseField("script_type");

        private final ClientProxy client;
        private final ScriptServiceProxy scriptService;

        @Inject
        public Parser(Settings settings, ClientProxy client, ScriptServiceProxy scriptService) {
            super(settings);
            this.client = client;
            this.scriptService = scriptService;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ScriptSearchTrigger parse(XContentParser parser) throws IOException {

            SearchRequest request = null;
            String scriptLang = null;
            String script = null;
            ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT) && currentFieldName != null) {
                    if (REQUEST_FIELD.match(currentFieldName)) {
                        request = AlertUtils.readSearchRequest(parser, AlertUtils.DEFAULT_TRIGGER_SEARCH_TYPE);
                    } else if (ScriptService.SCRIPT_ID.match(currentFieldName)) {
                        script = parser.text();
                        scriptType = ScriptService.ScriptType.INDEXED;
                    } else if (ScriptService.SCRIPT_INLINE.match(currentFieldName)) {
                        script = parser.text();
                    } else if (SCRIPT_TYPE_FIELD.match(currentFieldName)) {
                        scriptType = ScriptService.ScriptType.valueOf(parser.text());
                    } else if (ScriptService.SCRIPT_LANG.match(currentFieldName)) {
                        scriptLang = parser.text();
                    } else {
                        throw new TriggerException("could not parse script trigger. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (request == null) {
                throw new TriggerException("could not parse script trigger. missing required search request");
            }

            if (script == null) {
                throw new TriggerException("could not parse script trigger. either [script] or [script_id] must be provided");
            }

            return new ScriptSearchTrigger(logger, scriptService, client, request, script, scriptType, scriptLang);
        }

        @Override
        public ScriptSearchTrigger.Result parseResult(XContentParser parser) throws IOException {
            String currentFieldName = null;
            XContentParser.Token token;
            boolean triggered = false;
            Payload payload = null;
            SearchRequest request = null;
            String type = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (Trigger.Result.TYPE_FIELD.match(currentFieldName)) {
                        type = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Trigger.Result.TRIGGERED_FIELD.match(currentFieldName)) {
                            triggered = parser.booleanValue();
                        } else {
                            throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Trigger.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.Simple(parser.map()); ///@TODO FIXME
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        request = AlertUtils.readSearchRequest(parser);
                    } else {
                        throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new TriggerException("unable to parse trigger result. unexpected token [" + token + "]");
                }
            }
            return new Result(type, triggered, request, payload);
        }

    }
}
