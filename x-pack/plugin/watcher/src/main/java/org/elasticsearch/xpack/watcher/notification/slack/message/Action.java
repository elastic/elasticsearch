/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class Action implements MessageElement {

    static final ObjectParser<Template, Void> ACTION_PARSER = new ObjectParser<>("action", Template::new);
    static {
        ACTION_PARSER.declareField(Template::setType, (p, c) -> new TextTemplate(p.text()), new ParseField("type"), ValueType.STRING);
        ACTION_PARSER.declareField(Template::setUrl, (p, c) -> new TextTemplate(p.text()), new ParseField("url"), ValueType.STRING);
        ACTION_PARSER.declareField(Template::setText, (p, c) -> new TextTemplate(p.text()), new ParseField("text"), ValueType.STRING);
        ACTION_PARSER.declareField(Template::setStyle, (p, c) -> new TextTemplate(p.text()), new ParseField("style"), ValueType.STRING);
        ACTION_PARSER.declareField(Template::setName, (p, c) -> new TextTemplate(p.text()), new ParseField("name"), ValueType.STRING);
    }

    private static final ParseField URL = new ParseField("url");
    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField TEXT = new ParseField("text");
    private static final ParseField STYLE = new ParseField("style");
    private static final ParseField NAME = new ParseField("name");

    private String style;
    private String name;
    private String type;
    private String text;
    private String url;

    public Action() {}

    public Action(String style, String name, String type, String text, String url) {
        this.style = style;
        this.name = name;
        this.type = type;
        this.text = text;
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Action template = (Action) o;

        return Objects.equals(style, template.style)
            && Objects.equals(type, template.type)
            && Objects.equals(url, template.url)
            && Objects.equals(text, template.text)
            && Objects.equals(name, template.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(style, type, url, name, text);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(NAME.getPreferredName(), name)
            .field(STYLE.getPreferredName(), style)
            .field(TYPE.getPreferredName(), type)
            .field(TEXT.getPreferredName(), text)
            .field(URL.getPreferredName(), url)
            .endObject();
    }

    static class Template implements ToXContent {

        private TextTemplate type;
        private TextTemplate name;
        private TextTemplate text;
        private TextTemplate url;
        private TextTemplate style;

        public Action render(TextTemplateEngine engine, Map<String, Object> model) {
            String style = engine.render(this.style, model);
            String type = engine.render(this.type, model);
            String url = engine.render(this.url, model);
            String name = engine.render(this.name, model);
            String text = engine.render(this.text, model);
            return new Action(style, name, type, text, url);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;

            return Objects.equals(style, template.style)
                && Objects.equals(type, template.type)
                && Objects.equals(url, template.url)
                && Objects.equals(text, template.text)
                && Objects.equals(name, template.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(style, type, url, name, text);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(NAME.getPreferredName(), name)
                .field(STYLE.getPreferredName(), style)
                .field(TYPE.getPreferredName(), type)
                .field(TEXT.getPreferredName(), text)
                .field(URL.getPreferredName(), url)
                .endObject();
        }

        public TextTemplate getType() {
            return type;
        }

        public void setType(TextTemplate type) {
            this.type = type;
        }

        public TextTemplate getName() {
            return name;
        }

        public void setName(TextTemplate name) {
            this.name = name;
        }

        public TextTemplate getText() {
            return text;
        }

        public void setText(TextTemplate text) {
            this.text = text;
        }

        public TextTemplate getUrl() {
            return url;
        }

        public void setUrl(TextTemplate url) {
            this.url = url;
        }

        public TextTemplate getStyle() {
            return style;
        }

        public void setStyle(TextTemplate style) {
            this.style = style;
        }
    }
}
