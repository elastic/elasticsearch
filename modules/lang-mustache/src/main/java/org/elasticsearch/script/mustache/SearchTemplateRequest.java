/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to execute a search based on a search template.
 */
public class SearchTemplateRequest extends ActionRequest implements CompositeIndicesRequest, ToXContentObject {

    private SearchRequest request;
    private boolean simulate = false;
    private boolean explain = false;
    private boolean profile = false;
    private ScriptType scriptType;
    private String script;
    private Map<String, Object> scriptParams;

    public SearchTemplateRequest() {}

    public SearchTemplateRequest(StreamInput in) throws IOException {
        super(in);
        request = in.readOptionalWriteable(SearchRequest::new);
        simulate = in.readBoolean();
        explain = in.readBoolean();
        profile = in.readBoolean();
        scriptType = ScriptType.readFrom(in);
        script = in.readOptionalString();
        if (in.readBoolean()) {
            scriptParams = in.readMap();
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchTemplateRequest request1 = (SearchTemplateRequest) o;
        return simulate == request1.simulate &&
            explain == request1.explain &&
            profile == request1.profile &&
            Objects.equals(request, request1.request) &&
            scriptType == request1.scriptType &&
            Objects.equals(script, request1.script) &&
            Objects.equals(scriptParams, request1.scriptParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request, simulate, explain, profile, scriptType, script, scriptParams);
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

    private static ParseField ID_FIELD = new ParseField("id");
    private static ParseField SOURCE_FIELD = new ParseField("source", "inline", "template");

    private static ParseField PARAMS_FIELD = new ParseField("params");
    private static ParseField EXPLAIN_FIELD = new ParseField("explain");
    private static ParseField PROFILE_FIELD = new ParseField("profile");

    private static final ObjectParser<SearchTemplateRequest, Void> PARSER;
    static {
        PARSER = new ObjectParser<>("search_template");
        PARSER.declareField((parser, request, s) ->
                request.setScriptParams(parser.map())
            , PARAMS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareString((request, s) -> {
            request.setScriptType(ScriptType.STORED);
            request.setScript(s);
        }, ID_FIELD);
        PARSER.declareBoolean(SearchTemplateRequest::setExplain, EXPLAIN_FIELD);
        PARSER.declareBoolean(SearchTemplateRequest::setProfile, PROFILE_FIELD);
        PARSER.declareField((parser, request, value) -> {
            request.setScriptType(ScriptType.INLINE);
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                //convert the template to json which is the only supported XContentType (see CustomMustacheFactory#createEncoder)
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    request.setScript(Strings.toString(builder.copyCurrentStructure(parser)));
                } catch (IOException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Could not parse inline template", e);
                }
            } else {
                request.setScript(parser.text());
            }
        }, SOURCE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
    }

    public static SearchTemplateRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SearchTemplateRequest(), null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (scriptType == ScriptType.STORED) {
            builder.field(ID_FIELD.getPreferredName(), script);
        } else if (scriptType == ScriptType.INLINE) {
            builder.field(SOURCE_FIELD.getPreferredName(), script);
        } else {
            throw new UnsupportedOperationException("Unrecognized script type [" + scriptType + "].");
        }

        return builder.field(PARAMS_FIELD.getPreferredName(), scriptParams)
            .field(EXPLAIN_FIELD.getPreferredName(), explain)
            .field(PROFILE_FIELD.getPreferredName(), profile)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(request);
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
