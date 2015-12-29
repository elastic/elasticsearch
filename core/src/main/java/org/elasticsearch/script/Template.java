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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class Template extends Script {
    
    /** Default templating language */
    public static final String DEFAULT_LANG = "mustache";

    private XContentType contentType;

    public Template() {
        super();
    }

    /**
     * Constructor for simple inline template. The template will have no lang,
     * content type or params set.
     *
     * @param template
     *            The inline template.
     */
    public Template(String template) {
        super(template, DEFAULT_LANG);
    }

    /**
     * Constructor for Template.
     *
     * @param template
     *            The cache key of the template to be compiled/executed. For
     *            inline templates this is the actual templates source code. For
     *            indexed templates this is the id used in the request. For on
     *            file templates this is the file name.
     * @param type
     *            The type of template -- dynamic, indexed, or file.
     * @param lang
     *            The language of the template to be compiled/executed.
     * @param xContentType
     *            The {@link XContentType} of the template.
     * @param params
     *            The map of parameters the template will be executed with.
     */
    public Template(String template, ScriptType type, @Nullable String lang, @Nullable XContentType xContentType,
            @Nullable Map<String, Object> params) {
        super(template, type, lang == null ? DEFAULT_LANG : lang, params);
        this.contentType = xContentType;
    }

    /**
     * Method for getting the {@link XContentType} of the template.
     *
     * @return The {@link XContentType} of the template.
     */
    public XContentType getContentType() {
        return contentType;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.contentType = XContentType.readFrom(in);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        boolean hasContentType = contentType != null;
        out.writeBoolean(hasContentType);
        if (hasContentType) {
            XContentType.writeTo(contentType, out);
        }
    }

    @Override
    protected XContentBuilder scriptFieldToXContent(String template, ScriptType type, XContentBuilder builder, Params builderParams)
            throws IOException {
        if (type == ScriptType.INLINE && contentType != null && builder.contentType() == contentType) {
            builder.rawField(type.getParseField().getPreferredName(), new BytesArray(template));
        } else {
            builder.field(type.getParseField().getPreferredName(), template);
        }
        return builder;
    }

    public static Template readTemplate(StreamInput in) throws IOException {
        Template template = new Template();
        template.readFrom(in);
        return template;
    }

    public static Script parse(Map<String, Object> config, boolean removeMatchedEntries, ParseFieldMatcher parseFieldMatcher) {
        return new TemplateParser(Collections.emptyMap(), DEFAULT_LANG).parse(config, removeMatchedEntries, parseFieldMatcher);
    }

    public static Template parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return new TemplateParser(Collections.emptyMap(), DEFAULT_LANG).parse(parser, parseFieldMatcher);
    }

    @Deprecated
    public static Template parse(XContentParser parser, Map<String, ScriptType> additionalTemplateFieldNames, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return new TemplateParser(additionalTemplateFieldNames, DEFAULT_LANG).parse(parser, parseFieldMatcher);
    }

    @Deprecated
    public static Template parse(XContentParser parser, Map<String, ScriptType> additionalTemplateFieldNames, String defaultLang, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return new TemplateParser(additionalTemplateFieldNames, defaultLang).parse(parser, parseFieldMatcher);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((contentType == null) ? 0 : contentType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        Template other = (Template) obj;
        if (contentType != other.contentType)
            return false;
        return true;
    }

    private static class TemplateParser extends AbstractScriptParser<Template> {

        private XContentType contentType = null;
        private final Map<String, ScriptType> additionalTemplateFieldNames;
        private String defaultLang;

        public TemplateParser(Map<String, ScriptType> additionalTemplateFieldNames, String defaultLang) {
            this.additionalTemplateFieldNames = additionalTemplateFieldNames;
            this.defaultLang = defaultLang;
        }

        @Override
        protected Template createSimpleScript(XContentParser parser) throws IOException {
            return new Template(String.valueOf(parser.objectText()), ScriptType.INLINE, DEFAULT_LANG, contentType, null);
        }

        @Override
        protected Template createScript(String script, ScriptType type, String lang, Map<String, Object> params) {
            return new Template(script, type, lang, contentType, params);
        }

        @Override
        protected String parseInlineScript(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                contentType = parser.contentType();
                XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                return builder.copyCurrentStructure(parser).bytes().toUtf8();
            } else {
                return parser.text();
            }
        }

        @Override
        protected Map<String, ScriptType> getAdditionalScriptParameters() {
            return additionalTemplateFieldNames;
        }

        @Override
        protected String getDefaultScriptLang() {
            return defaultLang;
        }
    }
}
