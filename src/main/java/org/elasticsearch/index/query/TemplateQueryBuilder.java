/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

/**
 * Facilitates creating template query requests.
 * */
public class TemplateQueryBuilder extends BaseQueryBuilder {

    /** Parameters to fill the template with. */
    private Map<String, Object> vars;
    /** Template to fill.*/
    private String template;

    private ScriptService.ScriptType templateType;

    /**
     * @param template the template to use for that query.
     * @param vars the parameters to fill the template with.
     * */
    public TemplateQueryBuilder(String template, Map<String, Object> vars) {
        this(template, ScriptService.ScriptType.INLINE, vars);
    }

    /**
     * @param template the template to use for that query.
     * @param vars the parameters to fill the template with.
     * @param templateType what kind of template (INLINE,FILE,ID)
     * */
    public TemplateQueryBuilder(String template, ScriptService.ScriptType templateType, Map<String, Object> vars) {
        this.template = template;
        this.vars =vars;
        this.templateType = templateType;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TemplateQueryParser.NAME);
        String fieldname;
        switch(templateType){
            case FILE:
                fieldname = "file";
                break;
            case INDEXED:
                fieldname = "id";
                break;
            case INLINE:
                fieldname = TemplateQueryParser.QUERY;
                break;
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown template type " + templateType);
        }
        builder.field(fieldname, template);
        builder.field(TemplateQueryParser.PARAMS, vars);
        builder.endObject();
    }
}
