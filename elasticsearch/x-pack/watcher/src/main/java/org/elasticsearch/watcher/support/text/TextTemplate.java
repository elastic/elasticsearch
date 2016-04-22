/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.text;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class TextTemplate implements ToXContent {

    private final String template;
    private final @Nullable XContentType contentType;
    private final @Nullable ScriptType type;
    private final @Nullable Map<String, Object> params;

    public TextTemplate(String template) {
        this(template, null, null, null);
    }

    public TextTemplate(String template, @Nullable XContentType contentType, @Nullable ScriptType type,
                        @Nullable Map<String, Object> params) {
        this.template = template;
        this.contentType = contentType;
        this.type = type;
        this.params = params;
    }

    public String getTemplate() {
        return template;
    }

    public XContentType getContentType() {
        return contentType;
    }

    public ScriptType getType() {
        return type != null ? type : ScriptType.INLINE;
    }

    public Map<String, Object> getParams() {
        return params != null ? params : Collections.emptyMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextTemplate template1 = (TextTemplate) o;

        if (!template.equals(template1.template)) return false;
        if (contentType != template1.contentType) return false;
        if (type != template1.type) return false;
        return !(params != null ? !params.equals(template1.params) : template1.params != null);

    }

    @Override
    public int hashCode() {
        int result = template.hashCode();
        result = 31 * result + (contentType != null ? contentType.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == null) {
            return builder.value(template);
        }
        builder.startObject();
        switch (type) {
            case INLINE:
                if (contentType != null && builder.contentType() == contentType) {
                    builder.rawField(Field.INLINE.getPreferredName(), new BytesArray(template));
                } else {
                    builder.field(Field.INLINE.getPreferredName(), template);
                }
                break;
            case FILE:
                builder.field(Field.FILE.getPreferredName(), template);
                break;
            default: // STORED
                assert type == ScriptType.STORED : "template type [" + type + "] is not supported";
                builder.field(Field.ID.getPreferredName(), template);
        }
        if (this.params != null) {
            builder.field(Field.PARAMS.getPreferredName(), this.params);
        }
        return builder.endObject();
    }

    public static TextTemplate parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            return new TextTemplate(String.valueOf(parser.objectText()));
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
        }

        String template = null;
        XContentType contentType = null;
        ScriptType type = null;
        Map<String, Object> params = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.INLINE)) {
                type = ScriptType.INLINE;
                if (token.isValue()) {
                    template = String.valueOf(parser.objectText());
                } else {
                    contentType = parser.contentType();
                    XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                    template = builder.copyCurrentStructure(parser).bytes().toUtf8();
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.FILE)) {
                type = ScriptType.FILE;
                if (token == XContentParser.Token.VALUE_STRING) {
                    template = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]",
                            currentFieldName, token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ID)) {
                type = ScriptType.STORED;
                if (token == XContentParser.Token.VALUE_STRING) {
                    template = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]",
                            currentFieldName, token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.PARAMS)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ElasticsearchParseException("expected an object for field [{}], but found [{}]", currentFieldName, token);
                }
            } else {
                throw new ElasticsearchParseException("unexpected field [{}]", currentFieldName);
            }
        }
        if (template == null) {
            throw new ElasticsearchParseException("expected one of [{}], [{}] or [{}] fields, but found none",
                    Field.INLINE.getPreferredName(), Field.FILE.getPreferredName(), Field.ID.getPreferredName());
        }
        assert type != null : "if template is not null, type should definitely not be null";
        return new TextTemplate(template, contentType, type, params);
    }

    public static Builder inline(XContentBuilder template) {
        return new Builder.Inline(template.bytes().toUtf8()).contentType(template.contentType());
    }

    public static Builder<Builder.Inline> inline(String text) {
        return new Builder.Inline(text);
    }

    public static Builder<Builder.File> file(String file) {
        return new Builder.File(file);
    }

    public static Builder<Builder.Indexed> indexed(String id) {
        return new Builder.Indexed(id);
    }

    public static Builder.DefaultType defaultType(String text) {
        return new Builder.DefaultType(text);
    }

    public static abstract class Builder<B extends Builder> {

        protected final ScriptType type;
        protected final String template;
        protected Map<String, Object> params;

        protected Builder(String template, ScriptType type) {
            this.template = template;
            this.type = type;
        }

        public B params(Map<String, Object> params) {
            this.params = params;
            return (B) this;
        }

        public abstract TextTemplate build();

        public static class Inline extends Builder<Inline> {

            private XContentType contentType;

            public Inline(String script) {
                super(script, ScriptType.INLINE);
            }

            public Inline contentType(XContentType contentType) {
                this.contentType = contentType;
                return this;
            }

            @Override
            public TextTemplate build() {
                return new TextTemplate(template, contentType, type, params);
            }
        }

        public static class File extends Builder<File> {

            public File(String file) {
                super(file, ScriptType.FILE);
            }

            @Override
            public TextTemplate build() {
                return new TextTemplate(template, null, type, params);
            }
        }

        public static class Indexed extends Builder<Indexed> {

            public Indexed(String id) {
                super(id, ScriptType.STORED);
            }

            @Override
            public TextTemplate build() {
                return new TextTemplate(template, null, type, params);
            }
        }

        public static class DefaultType extends Builder<DefaultType> {

            public DefaultType(String text) {
                super(text, null);
            }

            @Override
            public TextTemplate build() {
                return new TextTemplate(template, null, type, params);
            }
        }
    }

    public interface Field {
        ParseField INLINE = new ParseField("inline");
        ParseField FILE = new ParseField("file");
        ParseField ID = new ParseField("id");
        ParseField PARAMS = new ParseField("params");
    }
}

