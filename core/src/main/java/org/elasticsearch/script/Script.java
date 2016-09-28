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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Script holds all the parameters necessary to compile or find in cache and then execute a script.
 */
public final class Script implements ToXContent, Writeable {

    public static final ScriptType DEFAULT_TYPE = ScriptType.INLINE;
    public static final String DEFAULT_SCRIPT_LANG = "painless";

    private String script;
    private ScriptType type;
    @Nullable private String lang;
    @Nullable private Map<String, Object> params;
    @Nullable private XContentType contentType;

    /**
     * Constructor for simple inline script. The script will have no lang or params set.
     *
     * @param script The inline script to execute.
     */
    public Script(String script) {
        this(script, ScriptType.INLINE, null, null);
    }

    public Script(String script, ScriptType type, String lang, @Nullable Map<String, ?> params) {
        this(script, type, lang, params, null);
    }

    /**
     * Constructor for Script.
     *
     * @param script        The cache key of the script to be compiled/executed. For inline scripts this is the actual
     *                      script source code. For indexed scripts this is the id used in the request. For on file
     *                      scripts this is the file name.
     * @param type          The type of script -- dynamic, stored, or file.
     * @param lang          The language of the script to be compiled/executed.
     * @param params        The map of parameters the script will be executed with.
     * @param contentType   The {@link XContentType} of the script. Only relevant for inline scripts that have not been
     *                      defined as a plain string, but as json or yaml content. This class needs this information
     *                      when serializing the script back to xcontent.
     */
    @SuppressWarnings("unchecked")
    public Script(String script, ScriptType type, String lang, @Nullable Map<String, ?> params,
                  @Nullable XContentType  contentType) {
        if (contentType != null && type != ScriptType.INLINE) {
            throw new IllegalArgumentException("The parameter contentType only makes sense for inline scripts");
        }
        this.script = Objects.requireNonNull(script);
        this.type = Objects.requireNonNull(type);
        this.lang = lang == null ? DEFAULT_SCRIPT_LANG : lang;
        this.params = (Map<String, Object>) params;
        this.contentType = contentType;
    }

    public Script(StreamInput in) throws IOException {
        script = in.readString();
        if (in.readBoolean()) {
            type = ScriptType.readFrom(in);
        }
        lang = in.readOptionalString();
        params = in.readMap();
        if (in.readBoolean()) {
            contentType = XContentType.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(script);
        boolean hasType = type != null;
        out.writeBoolean(hasType);
        if (hasType) {
            ScriptType.writeTo(type, out);
        }
        out.writeOptionalString(lang);
        out.writeMap(params);
        boolean hasContentType = contentType != null;
        out.writeBoolean(hasContentType);
        if (hasContentType) {
            XContentType.writeTo(contentType, out);
        }
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
     * @return The type of script -- inline, stored, or file.
     */
    public ScriptType getType() {
        return type;
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

    /**
     * @return The content type of the script if it is an inline script and the script has been defined as json
     *         or yaml content instead of a plain string.
     */
    public XContentType getContentType() {
        return contentType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        if (type == null) {
            return builder.value(script);
        }
        builder.startObject();
        if (type == ScriptType.INLINE && contentType != null && builder.contentType() == contentType) {
            builder.rawField(type.getParseField().getPreferredName(), new BytesArray(script));
        } else {
            builder.field(type.getParseField().getPreferredName(), script);
        }
        if (lang != null) {
            builder.field(ScriptField.LANG.getPreferredName(), lang);
        }
        if (params != null) {
            builder.field(ScriptField.PARAMS.getPreferredName(), params);
        }
        builder.endObject();
        return builder;
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return parse(parser, parseFieldMatcher, null);
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher, @Nullable String lang) throws IOException {
        XContentParser.Token token = parser.currentToken();
        // If the parser hasn't yet been pushed to the first token, do it now
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return new Script(parser.text(), ScriptType.INLINE, lang, null);
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
        }
        String script = null;
        ScriptType type = null;
        Map<String, Object> params = null;
        XContentType contentType = null;
        String cfn = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                cfn = parser.currentName();
            } else if (parseFieldMatcher.match(cfn, ScriptType.INLINE.getParseField())) {
                type = ScriptType.INLINE;
                if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    contentType = parser.contentType();
                    XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                    script = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                } else {
                    script = parser.text();
                }
            } else if (parseFieldMatcher.match(cfn, ScriptType.FILE.getParseField())) {
                type = ScriptType.FILE;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                }
            } else if (parseFieldMatcher.match(cfn, ScriptType.STORED.getParseField())) {
                type = ScriptType.STORED;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                }
            } else if (parseFieldMatcher.match(cfn, ScriptField.LANG)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    lang = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                }
            } else if (parseFieldMatcher.match(cfn, ScriptField.PARAMS)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ElasticsearchParseException("expected an object for field [{}], but found [{}]", cfn, token);
                }
            } else {
                throw new ElasticsearchParseException("unexpected field [{}]", cfn);
            }
        }
        if (script == null) {
            throw new ElasticsearchParseException("expected one of [{}], [{}] or [{}] fields, but found none",
                    ScriptType.INLINE.getParseField() .getPreferredName(), ScriptType.FILE.getParseField().getPreferredName(),
                    ScriptType.STORED.getParseField() .getPreferredName());
        }
        return new Script(script, type, lang, params, contentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lang, params, script, type, contentType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Script other = (Script) obj;

        return Objects.equals(lang, other.lang) &&
                Objects.equals(params, other.params) &&
                Objects.equals(script, other.script) &&
                Objects.equals(type, other.type) &&
                Objects.equals(contentType, other.contentType);
    }

    @Override
    public String toString() {
        return "[script: " + script + ", type: " + type.getParseField().getPreferredName() + ", lang: "
                + lang + ", params: " + params + "]";
    }

    public interface ScriptField {
        ParseField SCRIPT = new ParseField("script");
        ParseField LANG = new ParseField("lang");
        ParseField PARAMS = new ParseField("params");
    }

}
