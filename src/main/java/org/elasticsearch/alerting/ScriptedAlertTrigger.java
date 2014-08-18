/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

public class ScriptedAlertTrigger implements ToXContent {
    public String script;
    public ScriptService.ScriptType scriptType;
    public String scriptLang;


    public ScriptedAlertTrigger(String script, ScriptService.ScriptType scriptType, String scriptLang) {
        this.script = script;
        this.scriptType = scriptType;
        this.scriptLang = scriptLang;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("script",script);
        builder.field("script_type", scriptType);
        builder.field("script_lang", scriptLang);
        builder.endObject();
        return builder;
    }
}
