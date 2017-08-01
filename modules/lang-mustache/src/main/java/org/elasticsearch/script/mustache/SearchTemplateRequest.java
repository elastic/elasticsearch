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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to execute a search based on a search template.
 */
public class SearchTemplateRequest extends ActionRequest implements CompositeIndicesRequest {

    private SearchRequest request;
    private boolean simulate = false;
    private boolean explain = false;
    private boolean profile = false;
    private ScriptType scriptType;
    private String script;
    private Map<String, Object> scriptParams;

    public SearchTemplateRequest() {
    }

    public SearchTemplateRequest(SearchRequest searchRequest) {
        this.request = searchRequest;
    }

    public void setRequest(SearchRequest request) {
        this.request = request;
    }

    public SearchRequest getRequest() {
        return request;
    }


    public boolean isSimulate() {
        return simulate;
    }

    public void setSimulate(boolean simulate) {
        this.simulate = simulate;
    }

    public boolean isExplain() {
        return explain;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public boolean isProfile() {
        return profile;
    }

    public void setProfile(boolean profile) {
        this.profile = profile;
    }

    public ScriptType getScriptType() {
        return scriptType;
    }

    public void setScriptType(ScriptType scriptType) {
        this.scriptType = scriptType;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public Map<String, Object> getScriptParams() {
        return scriptParams;
    }

    public void setScriptParams(Map<String, Object> scriptParams) {
        this.scriptParams = scriptParams;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (script == null || script.isEmpty()) {
            validationException = addValidationError("template is missing", validationException);
        }
        if (scriptType == null) {
            validationException = addValidationError("template's script type is missing", validationException);
        }
        if (simulate == false) {
            if (request == null) {
                validationException = addValidationError("search request is missing", validationException);
            } else {
                ActionRequestValidationException ex = request.validate();
                if (ex != null) {
                    if (validationException == null) {
                        validationException = new ActionRequestValidationException();
                    }
                    validationException.addValidationErrors(ex.validationErrors());
                }
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = in.readOptionalStreamable(SearchRequest::new);
        simulate = in.readBoolean();
        explain = in.readBoolean();
        profile = in.readBoolean();
        scriptType = ScriptType.readFrom(in);
        script = in.readOptionalString();
        if (in.readBoolean()) {
            scriptParams = in.readMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(request);
        out.writeBoolean(simulate);
        out.writeBoolean(explain);
        out.writeBoolean(profile);
        scriptType.writeTo(out);
        out.writeOptionalString(script);
        boolean hasParams = scriptParams != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(scriptParams);
        }
    }
}
