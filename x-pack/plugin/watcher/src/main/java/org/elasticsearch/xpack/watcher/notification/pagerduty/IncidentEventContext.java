/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class IncidentEventContext implements ToXContentObject {

    enum Type {
        LINK, IMAGE
    }

    final Type type;
    final String href;
    final String text;
    final String src;
    final String alt;

    public static IncidentEventContext link(String href, @Nullable String text) {
        assert href != null;
        return new IncidentEventContext(Type.LINK, href, text, null, null);
    }

    public static IncidentEventContext image(String src, @Nullable String href, @Nullable String alt) {
        assert src != null;
        return new IncidentEventContext(Type.IMAGE, href, null, src, alt);
    }

    private IncidentEventContext(Type type, String href, String text, String src, String alt) {
        this.type = type;
        this.href = href;
        this.text = text;
        this.src = src;
        this.alt = alt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IncidentEventContext that = (IncidentEventContext) o;

        return Objects.equals(type, that.type) &&
                Objects.equals(href, that.href) &&
                Objects.equals(text, that.text) &&
                Objects.equals(src, that.src) &&
                Objects.equals(alt, that.alt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, href, text, src, alt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(XField.TYPE.getPreferredName(), type.name().toLowerCase(Locale.ROOT));
        switch (type) {
            case LINK:
                builder.field(XField.HREF.getPreferredName(), href);
                if (text != null) {
                    builder.field(XField.TEXT.getPreferredName(), text);
                }
                break;
            case IMAGE:
                builder.field(XField.SRC.getPreferredName(), src);
                if (href != null) {
                    builder.field(XField.HREF.getPreferredName(), href);
                }
                if (alt != null) {
                    builder.field(XField.ALT.getPreferredName(), alt);
                }
        }
        return builder.endObject();
    }

    public static IncidentEventContext parse(XContentParser parser) throws IOException {
        Type type = null;
        String href = null;
        String text = null;
        String src = null;
        String alt = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Strings.hasLength(currentFieldName)) {
                if (XField.TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        type = Type.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException e) {
                        String msg = "could not parse trigger incident event context. unknown context type [{}]";
                        throw new ElasticsearchParseException(msg, parser.text());
                    }
                } else {
                    if (XField.HREF.match(currentFieldName, parser.getDeprecationHandler())) {
                        href = parser.text();
                    } else if (XField.TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                        text = parser.text();
                    } else if (XField.SRC.match(currentFieldName, parser.getDeprecationHandler())) {
                        src = parser.text();
                    } else if (XField.ALT.match(currentFieldName, parser.getDeprecationHandler())) {
                        alt = parser.text();
                    } else {
                        String msg = "could not parse trigger incident event context. unknown field [{}]";
                        throw new ElasticsearchParseException(msg, currentFieldName);
                    }
                }
            }
        }

        return createAndValidateTemplate(type, href, src, alt, text);
    }

    private static IncidentEventContext createAndValidateTemplate(Type type, String href, String src, String alt,
                                                      String text) {
        if (type == null) {
            throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field [{}]",
                XField.TYPE.getPreferredName());
        }

        switch (type) {
            case LINK:
                if (href == null) {
                    throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field " +
                        "[{}] for [{}] context", XField.HREF.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                }
                if (src != null) {
                    throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                        "[{}] context", XField.SRC.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                }
                if (alt != null) {
                    throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                        "[{}] context", XField.ALT.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                }
                return link(href, text);
            case IMAGE:
                if (src == null) {
                    throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field " +
                        "[{}] for [{}] context", XField.SRC.getPreferredName(), Type.IMAGE.name().toLowerCase(Locale.ROOT));
                }
                if (text != null) {
                    throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                        "[{}] context", XField.TEXT.getPreferredName(), Type.IMAGE.name().toLowerCase(Locale.ROOT));
                }
                return image(src, href, alt);
            default:
                throw new ElasticsearchParseException("could not parse trigger incident event context. unknown context type [{}]",
                    type);
        }
    }


    public static class Template implements ToXContentObject {

        final Type type;
        final TextTemplate href;
        final TextTemplate text;
        final TextTemplate src;
        final TextTemplate alt;

        public static Template link(TextTemplate href, @Nullable TextTemplate text) {
            if (href == null) {
                throw new IllegalStateException("could not create link context for pager duty trigger incident event. missing required " +
                        "[href] setting");
            }
            return new Template(Type.LINK, href, text, null, null);
        }

        public static Template image(TextTemplate src, @Nullable TextTemplate href, @Nullable TextTemplate alt) {
            if (src == null) {
                throw new IllegalStateException("could not create link context for pager duty trigger incident event. missing required " +
                        "[src] setting");
            }
            return new Template(Type.IMAGE, href, null, src, alt);
        }

        private Template(Type type, TextTemplate href, TextTemplate text, TextTemplate src, TextTemplate alt) {
            this.type = type;
            this.href = href;
            this.text = text;
            this.src = src;
            this.alt = alt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template that = (Template) o;
            return Objects.equals(type, that.type) &&
                    Objects.equals(href, that.href) &&
                    Objects.equals(text, that.text) &&
                    Objects.equals(src, that.src) &&
                    Objects.equals(alt, that.alt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, href, text, src, alt);
        }

        public IncidentEventContext render(TextTemplateEngine engine, Map<String, Object> model, IncidentEventDefaults defaults) {
            switch (type) {
                case LINK:
                    String href = this.href != null ? engine.render(this.href, model) : defaults.link.href;
                    String text = this.text != null ? engine.render(this.text, model) : defaults.link.text;
                    return IncidentEventContext.link(href, text);

                default:
                    assert type == Type.IMAGE;
                    String src = this.src != null ? engine.render(this.src, model) : defaults.image.src;
                    href = this.href != null ? engine.render(this.href, model) : defaults.image.href;
                    String alt = this.alt != null ? engine.render(this.alt, model) : defaults.image.alt;
                    return IncidentEventContext.image(src, href, alt);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(XField.TYPE.getPreferredName(), type.name().toLowerCase(Locale.ROOT));
            switch (type) {
                case LINK:
                    builder.field(XField.HREF.getPreferredName(), href, params);
                    if (text != null) {
                        builder.field(XField.TEXT.getPreferredName(), text, params);
                    }
                    break;
                case IMAGE:
                    builder.field(XField.SRC.getPreferredName(), src, params);
                    if (href != null) {
                        builder.field(XField.HREF.getPreferredName(), href, params);
                    }
                    if (alt != null) {
                        builder.field(XField.ALT.getPreferredName(), alt, params);
                    }
            }
            return builder.endObject();
        }

        public static Template parse(XContentParser parser) throws IOException {
            Type type = null;
            TextTemplate href = null;
            TextTemplate text = null;
            TextTemplate src = null;
            TextTemplate alt = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Strings.hasLength(currentFieldName)) {
                    if (XField.TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        try {
                            type = Type.valueOf(parser.text().toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException e) {
                            String msg = "could not parse trigger incident event context. unknown context type [{}]";
                            throw new ElasticsearchParseException(msg, parser.text());
                        }
                    } else {
                        TextTemplate parsedTemplate;
                        try {
                            parsedTemplate = TextTemplate.parse(parser);
                        } catch (ElasticsearchParseException e) {
                            String msg = "could not parse trigger incident event context. failed to parse [{}] field";
                            throw new ElasticsearchParseException(msg, e, currentFieldName);
                        }

                        if (XField.HREF.match(currentFieldName, parser.getDeprecationHandler())) {
                            href = parsedTemplate;
                        } else if (XField.TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                            text = parsedTemplate;
                        } else if (XField.SRC.match(currentFieldName, parser.getDeprecationHandler())) {
                            src = parsedTemplate;
                        } else if (XField.ALT.match(currentFieldName, parser.getDeprecationHandler())) {
                            alt = parsedTemplate;
                        } else {
                            String msg = "could not parse trigger incident event context. unknown field [{}]";
                            throw new ElasticsearchParseException(msg, currentFieldName);
                        }
                    }
                }
            }

            return createAndValidateTemplate(type, href, src, alt, text);
        }

        private static Template createAndValidateTemplate(Type type, TextTemplate href, TextTemplate src, TextTemplate alt,
                                                          TextTemplate text) {
            if (type == null) {
                throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field [{}]",
                        XField.TYPE.getPreferredName());
            }

            switch (type) {
                case LINK:
                    if (href == null) {
                        throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field " +
                                "[{}] for [{}] context", XField.HREF.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                    }
                    if (src != null) {
                        throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                                "[{}] context", XField.SRC.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                    }
                    if (alt != null) {
                        throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                                "[{}] context", XField.ALT.getPreferredName(), Type.LINK.name().toLowerCase(Locale.ROOT));
                    }
                    return link(href, text);
                case IMAGE:
                    if (src == null) {
                        throw new ElasticsearchParseException("could not parse trigger incident event context. missing required field " +
                                "[{}] for [{}] context", XField.SRC.getPreferredName(), Type.IMAGE.name().toLowerCase(Locale.ROOT));
                    }
                    if (text != null) {
                        throw new ElasticsearchParseException("could not parse trigger incident event context. unexpected field [{}] for " +
                                "[{}] context", XField.TEXT.getPreferredName(), Type.IMAGE.name().toLowerCase(Locale.ROOT));
                    }
                    return image(src, href, alt);
                default:
                    throw new ElasticsearchParseException("could not parse trigger incident event context. unknown context type [{}]",
                            type);
            }
        }

    }

    interface XField {
        ParseField TYPE = new ParseField("type");
        ParseField HREF = new ParseField("href");

        // "link" context fields
        ParseField TEXT = new ParseField("text");

        // "image" context fields
        ParseField SRC = new ParseField("src");
        ParseField ALT = new ParseField("alt");

    }
}
