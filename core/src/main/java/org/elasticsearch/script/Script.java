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

package org.elasticsearch.script;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Map;

/**
 * Script holds all the parameters necessary to compile or find in cache and then execute a script.
 */
public class Script implements ToXContent, Streamable {

    public static final ScriptType DEFAULT_TYPE = ScriptType.INLINE;
    private static final ScriptParser PARSER = new ScriptParser();

    private String script;
    private @Nullable ScriptType type;
    private @Nullable String lang;
    private @Nullable Map<String, Object> params;

    /**
     * For Serialization
     */
    Script() {
    }

    /**
     * Constructor for simple inline script. The script will have no lang or
     * params set.
     *
     * @param script
     *            The inline script to execute.
     */
    public Script(String script) {
        this(script, null);
    }

    /**
     * For sub-classes to use to override the default language
     */
    protected Script(String script, String lang) {
        this(script, ScriptType.INLINE, lang, null);
    }

    /**
     * Constructor for Script.
     * 
     * @param script
     *            The cache key of the script to be compiled/executed. For
     *            inline scripts this is the actual script source code. For
     *            indexed scripts this is the id used in the request. For on
     *            file scripts this is the file name.
     * @param type
     *            The type of script -- dynamic, indexed, or file.
     * @param lang
     *            The language of the script to be compiled/executed.
     * @param params
     *            The map of parameters the script will be executed with.
     */
    public Script(String script, ScriptType type, @Nullable String lang, @Nullable Map<String, ? extends Object> params) {
        if (script == null) {
            throw new IllegalArgumentException("The parameter script (String) must not be null in Script.");
        }
        if (type == null) {
            throw new IllegalArgumentException("The parameter type (ScriptType) must not be null in Script.");
        }
        this.script = script;
        this.type = type;
        this.lang = lang;
        this.params = (Map<String, Object>)params;
    }

    /**
     * Method for getting the script.
     * @return The cache key of the script to be compiled/executed.  For dynamic scripts this is the actual
     *         script source code.  For indexed scripts this is the id used in the request.  For on disk scripts
     *         this is the file name.
     */
    public String getScript() {
        return script;
    }

    /**
     * Method for getting the type.
     * 
     * @return The type of script -- inline, indexed, or file.
     */
    public ScriptType getType() {
        return type == null ? DEFAULT_TYPE : type;
    }

    /**
     * Method for getting language.
     * 
     * @return The language of the script to be compiled/executed.
     */
    public String getLang() {
        return lang;
    }

    /**
     * Method for getting the parameters.
     * 
     * @return The map of parameters the script will be executed with.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        script = in.readString();
        if (in.readBoolean()) {
            type = ScriptType.readFrom(in);
        }
        lang = in.readOptionalString();
        if (in.readBoolean()) {
            params = in.readMap();
        }
        doReadFrom(in);
    }

    protected void doReadFrom(StreamInput in) throws IOException {
        // For sub-classes to Override
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(script);
        boolean hasType = type != null;
        out.writeBoolean(hasType);
        if (hasType) {
            ScriptType.writeTo(type, out);
        }
        out.writeOptionalString(lang);
        boolean hasParams = params != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(params);
        }
        doWriteTo(out);
    }

    protected void doWriteTo(StreamOutput out) throws IOException {
        // For sub-classes to Override
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        if (type == null) {
            return builder.value(script);
        }

        builder.startObject();
        scriptFieldToXContent(script, type, builder, builderParams);
        if (lang != null) {
            builder.field(ScriptField.LANG.getPreferredName(), lang);
        }
        if (params != null) {
            builder.field(ScriptField.PARAMS.getPreferredName(), params);
        }
        builder.endObject();
        return builder;
    }

    protected XContentBuilder scriptFieldToXContent(String script, ScriptType type, XContentBuilder builder, Params builderParams)
            throws IOException {
        builder.field(type.getParseField().getPreferredName(), script);
        return builder;
    }

    public static Script readScript(StreamInput in) throws IOException {
        Script script = new Script();
        script.readFrom(in);
        return script;
    }

    public static Script parse(Map<String, Object> config, boolean removeMatchedEntries, ParseFieldMatcher parseFieldMatcher) {
        return PARSER.parse(config, removeMatchedEntries, parseFieldMatcher);
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return PARSER.parse(parser, parseFieldMatcher);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lang == null) ? 0 : lang.hashCode());
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        result = prime * result + ((script == null) ? 0 : script.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Script other = (Script) obj;
        if (lang == null) {
            if (other.lang != null)
                return false;
        } else if (!lang.equals(other.lang))
            return false;
        if (params == null) {
            if (other.params != null)
                return false;
        } else if (!params.equals(other.params))
            return false;
        if (script == null) {
            if (other.script != null)
                return false;
        } else if (!script.equals(other.script))
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[script: " + script + ", type: " + type.getParseField().getPreferredName() + ", lang: " + lang + ", params: " + params
                + "]";
    }

    private static class ScriptParser extends AbstractScriptParser<Script> {

        @Override
        protected Script createSimpleScript(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new Script(parser.text());
            } else {
                throw new ScriptParseException("expected a string value for field [{}], but found [{}]", parser.currentName(),
                        parser.currentToken());
            }
        }

        @Override
        protected Script createScript(String script, ScriptType type, String lang, Map<String, Object> params) {
            return new Script(script, type, lang, params);
        }

        @Override
        protected String parseInlineScript(XContentParser parser) throws IOException {
            return parser.text();
        }
    }

    public interface ScriptField {
        ParseField SCRIPT = new ParseField("script");
        ParseField LANG = new ParseField("lang");
        ParseField PARAMS = new ParseField("params");
    }

    public static class ScriptParseException extends ElasticsearchException {

        public ScriptParseException(String msg, Object... args) {
            super(LoggerMessageFormat.format(msg, args));
        }

        public ScriptParseException(StreamInput in) throws IOException{
            super(in);
        }
    }
}
