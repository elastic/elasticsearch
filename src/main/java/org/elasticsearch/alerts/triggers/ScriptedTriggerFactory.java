/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

public class ScriptedTriggerFactory implements TriggerFactory {
    private final ScriptService scriptService;

    public ScriptedTriggerFactory(ScriptService service) {
        scriptService = service;
    }

    @Override
    public AlertTrigger createTrigger(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        String scriptLang = null;
        String script = null;
        ScriptService.ScriptType scriptType = null;
        ESLogger logger = Loggers.getLogger(this.getClass());
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                logger.error("FOOOBAR : [{}]", currentFieldName);
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "script_id" :
                        script = parser.text();
                        //@TODO assert script type was null or INDEXED already
                        scriptType = ScriptService.ScriptType.INDEXED;
                        break;
                    case "script" :
                        script = parser.text();
                        scriptType = ScriptService.ScriptType.INLINE;
                        break;
                    case "script_type" :
                        ScriptService.ScriptType tmpType = ScriptService.ScriptType.valueOf(parser.text());
                        if (scriptType == ScriptService.ScriptType.INDEXED && tmpType != scriptType) {
                            throw new ElasticsearchException("Unexpected script type for script_id [" + tmpType + "]");
                        } else {
                            scriptType = tmpType;
                        }
                        break;
                    case "script_lang" :
                        scriptLang = parser.text();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            }
        }
        if (script == null) {
            throw new ElasticsearchException("Failed to parse ScriptedTrigger script:[" + script
                    + "] scriptLang:[" + scriptLang + "] scriptType:[" + scriptType + "]");
        }
        return new ScriptedTrigger(script, scriptType, scriptLang);
    }

    @Override
    public boolean isTriggered(AlertTrigger trigger, SearchRequest request, SearchResponse response) {
        if (! (trigger instanceof ScriptedTrigger) ){
            throw new ElasticsearchIllegalStateException("Failed to evaluate isTriggered expected type ["
                    + ScriptedTrigger.class + "] got [" + trigger.getClass() + "]");
        }

        ScriptedTrigger scriptedTrigger = (ScriptedTrigger)trigger;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            Map<String, Object> responseMap = XContentHelper.convertToMap(builder.bytes(), false).v2();


            ExecutableScript executable = scriptService.executable(
                    scriptedTrigger.getScriptLang(), scriptedTrigger.getScript(), scriptedTrigger.getScriptType(), responseMap
            );

            Object returnValue = executable.run();
            if (returnValue instanceof Boolean) {
                return (Boolean) returnValue;
            } else {
                throw new ElasticsearchIllegalStateException("Trigger script [" + scriptedTrigger + "] did not return a Boolean");
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to execute trigger", e);
        }

    }

}
