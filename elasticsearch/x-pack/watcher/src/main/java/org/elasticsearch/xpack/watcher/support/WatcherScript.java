/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.ScriptSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

// TODO: remove this class as it is exactly the same as org.elasticsearch.script.Script
// and we should be able to remove it without breaking bwc in the .watch index
public class WatcherScript implements ToXContent {

    public static final String DEFAULT_LANG = ScriptSettings.DEFAULT_LANG;
    public static final ScriptContext.Plugin CTX_PLUGIN = new ScriptContext.Plugin("xpack", "watch");
    public static final ScriptContext CTX = new WatcherScriptContext();

    private final String script;
    @Nullable private final ScriptType type;
    @Nullable private final String lang;
    @Nullable private final Map<String, Object> params;

    WatcherScript(String script) {
        this(script, null, null, null);
    }

    WatcherScript(String script, @Nullable ScriptType type, @Nullable String lang, @Nullable Map<String, Object> params) {
        this.script = script;
        this.type = type;
        this.lang = lang;
        this.params = params;
    }

    public String script() {
        return script;
    }

    public ScriptType type() {
        return type != null ? type : ScriptType.INLINE;
    }

    public String lang() {
        return lang != null ? lang : DEFAULT_LANG;
    }

    public Map<String, Object> params() {
        return params != null ? params : Collections.emptyMap();
    }

    public Script toScript() {
        return new Script(script(), type(), lang(), params());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherScript script1 = (WatcherScript) o;

        if (!script.equals(script1.script)) return false;
        if (type != script1.type) return false;
        if (lang != null ? !lang.equals(script1.lang) : script1.lang != null) return false;
        return !(params != null ? !params.equals(script1.params) : script1.params != null);
    }

    @Override
    public int hashCode() {
        int result = script.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (lang != null ? lang.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == null) {
            return builder.value(script);
        }
        builder.startObject();
        switch (type) {
            case INLINE:
                builder.field(Field.INLINE.getPreferredName(), script);
                break;
            case FILE:
                builder.field(Field.FILE.getPreferredName(), script);
                break;
            default:
                assert type == ScriptType.STORED : "script type [" + type + "] is not supported";
                builder.field(Field.ID.getPreferredName(), script);
        }
        if (lang != null) {
            builder.field(Field.LANG.getPreferredName(), lang);
        }
        if (this.params != null) {
            builder.field(Field.PARAMS.getPreferredName(), this.params);
        }
        return builder.endObject();
    }

    public static WatcherScript parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new WatcherScript(parser.text());
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
        }

        String script = null;
        ScriptType type = null;
        String lang = null;
        Map<String, Object> params = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.INLINE)) {
                type = ScriptType.INLINE;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", currentFieldName,
                            token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.FILE)) {
                type = ScriptType.FILE;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", currentFieldName,
                            token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ID)) {
                type = ScriptType.STORED;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", currentFieldName,
                            token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.LANG)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    lang = parser.text();
                } else {
                    throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", currentFieldName,
                            token);
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
        if (script == null) {
            throw new ElasticsearchParseException("expected one of [{}], [{}] or [{}] fields, but found none",
                    Field.INLINE.getPreferredName(), Field.FILE.getPreferredName(), Field.ID.getPreferredName());
        }
        assert type != null : "if script is not null, type should definitely not be null";
        return new WatcherScript(script, type, lang, params);
    }

    public static Builder.Inline inline(String script) {
        return new Builder.Inline(script);
    }

    public static Builder.File file(String file) {
        return new Builder.File(file);
    }

    public static Builder.Indexed indexed(String id) {
        return new Builder.Indexed(id);
    }

    public static Builder.DefaultType defaultType(String text) {
        return new Builder.DefaultType(text);
    }

    public abstract static class Builder<B extends Builder> {

        protected final ScriptType type;
        protected final String script;
        protected String lang;
        protected Map<String, Object> params;

        protected Builder(String script, ScriptType type) {
            this.script = script;
            this.type = type;
        }

        public B lang(String lang) {
            this.lang = lang;
            return (B) this;
        }

        public B params(Map<String, Object> params) {
            this.params = params;
            return (B) this;
        }

        public abstract WatcherScript build();

        public static class Inline extends Builder<Inline> {

            public Inline(String script) {
                super(script, ScriptType.INLINE);
            }

            @Override
            public WatcherScript build() {
                return new WatcherScript(script, type, lang, params);
            }
        }

        public static class File extends Builder<File> {

            public File(String file) {
                super(file, ScriptType.FILE);
            }

            @Override
            public WatcherScript build() {
                return new WatcherScript(script, type, lang, params);
            }
        }

        public static class Indexed extends Builder<Indexed> {

            public Indexed(String id) {
                super(id, ScriptType.STORED);
            }

            @Override
            public WatcherScript build() {
                return new WatcherScript(script, type, lang, params);
            }
        }

        public static class DefaultType extends Builder<DefaultType> {

            public DefaultType(String text) {
                super(text, null);
            }

            @Override
            public WatcherScript build() {
                return new WatcherScript(script, type, lang, params);
            }
        }
    }

    interface Field {
        ParseField INLINE = new ParseField("inline");
        ParseField FILE = new ParseField("file");
        ParseField ID = new ParseField("id");
        ParseField LANG = new ParseField("lang");
        ParseField PARAMS = new ParseField("params");
    }


    private static class WatcherScriptContext implements ScriptContext {
        @Override
        public String getKey() {
            return CTX_PLUGIN.getKey();
        }
    }
}
