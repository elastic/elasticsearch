/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SlackMessage implements MessageElement {

    final String from;
    final String[] to;
    final String icon;
    final String text;
    final Attachment[] attachments;

    public SlackMessage(String from, String[] to, String icon, @Nullable String text, @Nullable Attachment[] attachments) {
        if (text == null && attachments == null) {
            throw new IllegalArgumentException("Both text and attachments cannot be null.");
        }

        this.from = from;
        this.to = to;
        this.icon = icon;
        this.text = text;
        this.attachments = attachments;
    }

    public String getFrom() {
        return from;
    }

    public String[] getTo() {
        return to;
    }

    public String getIcon() {
        return icon;
    }

    public String getText() {
        return text;
    }

    public Attachment[] getAttachments() {
        return attachments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SlackMessage that = (SlackMessage) o;

        return Objects.equals(from, that.from)
            && Arrays.equals(to, that.to)
            && Objects.equals(icon, that.icon)
            && Objects.equals(text, that.text)
            && Arrays.equals(attachments, that.attachments);
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? Arrays.hashCode(to) : 0);
        result = 31 * result + (icon != null ? icon.hashCode() : 0);
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (attachments != null ? Arrays.hashCode(attachments) : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, true);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean includeTargets) throws IOException {
        builder.startObject();
        if (from != null) {
            builder.field(XField.FROM.getPreferredName(), from);
        }
        if (includeTargets) {
            if (to != null) {
                builder.array(XField.TO.getPreferredName(), to);
            }
        }
        if (icon != null) {
            builder.field(XField.ICON.getPreferredName(), icon);
        }
        if (text != null) {
            builder.field(XField.TEXT.getPreferredName(), text);
        }
        if (attachments != null) {
            builder.startArray(XField.ATTACHMENTS.getPreferredName());
            for (Attachment attachment : attachments) {
                attachment.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public static class Template implements ToXContentObject {

        final TextTemplate from;
        final TextTemplate[] to;
        final TextTemplate text;
        final TextTemplate icon;
        final Attachment.Template[] attachments;
        final DynamicAttachments dynamicAttachments;

        public Template(
            TextTemplate from,
            TextTemplate[] to,
            TextTemplate text,
            TextTemplate icon,
            Attachment.Template[] attachments,
            DynamicAttachments dynamicAttachments
        ) {
            this.from = from;
            this.to = to;
            this.text = text;
            this.icon = icon;
            this.attachments = attachments;
            this.dynamicAttachments = dynamicAttachments;
        }

        public TextTemplate getFrom() {
            return from;
        }

        public TextTemplate[] getTo() {
            return to;
        }

        public TextTemplate getText() {
            return text;
        }

        public TextTemplate getIcon() {
            return icon;
        }

        public Attachment.Template[] getAttachments() {
            return attachments;
        }

        public DynamicAttachments dynamicAttachments() {
            return dynamicAttachments;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;

            return Objects.equals(from, template.from)
                && Objects.equals(text, template.text)
                && Objects.equals(icon, template.icon)
                && Objects.equals(dynamicAttachments, template.dynamicAttachments)
                && Arrays.equals(to, template.to)
                && Arrays.equals(attachments, template.attachments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, Arrays.hashCode(to), text, icon, Arrays.hashCode(attachments), dynamicAttachments);
        }

        public SlackMessage render(
            String watchId,
            String actionId,
            TextTemplateEngine engine,
            Map<String, Object> model,
            SlackMessageDefaults defaults
        ) {
            String from = this.from != null ? engine.render(this.from, model) : defaults.from != null ? defaults.from : watchId;
            String[] to = defaults.to;
            if (this.to != null) {
                to = new String[this.to.length];
                for (int i = 0; i < to.length; i++) {
                    to[i] = engine.render(this.to[i], model);
                }
            }
            String text = this.text != null ? engine.render(this.text, model) : defaults.text;
            String icon = this.icon != null ? engine.render(this.icon, model) : defaults.icon;
            List<Attachment> attachments = null;
            if (this.attachments != null) {
                attachments = new ArrayList<>();
                for (Attachment.Template attachment : this.attachments) {
                    attachments.add(attachment.render(engine, model, defaults.attachment));
                }
            }
            if (dynamicAttachments != null) {
                if (attachments == null) {
                    attachments = new ArrayList<>();
                }
                attachments.addAll(dynamicAttachments.render(engine, model, defaults.attachment));
            }
            if (attachments == null) {
                return new SlackMessage(from, to, icon, text, null);
            }
            return new SlackMessage(from, to, icon, text, attachments.toArray(new Attachment[attachments.size()]));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (from != null) {
                builder.field(XField.FROM.getPreferredName(), from);
            }
            if (to != null) {
                builder.startArray(XField.TO.getPreferredName());
                for (TextTemplate template : to) {
                    template.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (text != null) {
                builder.field(XField.TEXT.getPreferredName(), text, params);
            }
            if (icon != null) {
                builder.field(XField.ICON.getPreferredName(), icon, params);
            }
            if (attachments != null) {
                builder.startArray(XField.ATTACHMENTS.getPreferredName());
                for (Attachment.Template attachment : attachments) {
                    attachment.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (dynamicAttachments != null) {
                builder.field(XField.DYNAMIC_ATTACHMENTS.getPreferredName(), dynamicAttachments, params);
            }
            return builder.endObject();
        }

        public static Template parse(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (XField.FROM.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        builder.setFrom(TextTemplate.parse(parser));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse slack message. failed to parse [{}] field",
                            pe,
                            XField.FROM.getPreferredName()
                        );
                    }
                } else if (XField.TO.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                builder.addTo(TextTemplate.parse(parser));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException(
                                    "could not parse slack message. failed to parse [{}] field.",
                                    pe,
                                    XField.TO.getPreferredName()
                                );
                            }
                        }
                    } else {
                        try {
                            builder.addTo(TextTemplate.parse(parser));
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException(
                                "could not parse slack message. failed to parse [{}] field",
                                pe,
                                XField.TO.getPreferredName()
                            );
                        }
                    }
                } else if (XField.TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        builder.setText(TextTemplate.parse(parser));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse slack message. failed to parse [{}] field",
                            pe,
                            XField.TEXT.getPreferredName()
                        );
                    }
                } else if (XField.ICON.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        builder.setIcon(TextTemplate.parse(parser));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse slack message. failed to parse [{}] field.",
                            pe,
                            XField.ICON.getPreferredName()
                        );
                    }
                } else if (XField.ATTACHMENTS.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                builder.addAttachments(Attachment.Template.parse(parser));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException(
                                    "could not parse slack message. failed to parse [{}] field.",
                                    pe,
                                    XField.ATTACHMENTS.getPreferredName()
                                );
                            }
                        }
                    } else {
                        try {
                            builder.addAttachments(Attachment.Template.parse(parser));
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException(
                                "could not parse slack message. failed to parse [{}] field.",
                                pe,
                                XField.ATTACHMENTS.getPreferredName()
                            );
                        }
                    }
                } else if (XField.DYNAMIC_ATTACHMENTS.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        builder.setDynamicAttachments(DynamicAttachments.parse(parser));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse slack message. failed to parse [{}] field.",
                            pe,
                            XField.ICON.getPreferredName()
                        );
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse slack message. unknown field [{}].", currentFieldName);
                }
            }

            return builder.build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            TextTemplate from;
            final List<TextTemplate> to = new ArrayList<>();
            TextTemplate text;
            TextTemplate icon;
            final List<Attachment.Template> attachments = new ArrayList<>();
            DynamicAttachments dynamicAttachments;

            private Builder() {}

            public Builder setFrom(TextTemplate from) {
                this.from = from;
                return this;
            }

            public Builder setFrom(String from) {
                return setFrom(new TextTemplate(from));
            }

            public Builder addTo(TextTemplate... to) {
                Collections.addAll(this.to, to);
                return this;
            }

            public Builder addTo(String... to) {
                for (String name : to) {
                    this.to.add(new TextTemplate(name));
                }
                return this;
            }

            public Builder setText(TextTemplate text) {
                this.text = text;
                return this;
            }

            public Builder setText(String text) {
                return setText(new TextTemplate(text));
            }

            public Builder setIcon(TextTemplate icon) {
                this.icon = icon;
                return this;
            }

            public Builder setIcon(String icon) {
                return setIcon(new TextTemplate(icon));
            }

            public Builder addAttachments(Attachment.Template... attachments) {
                Collections.addAll(this.attachments, attachments);
                return this;
            }

            public Builder addAttachments(Attachment.Template.Builder... attachments) {
                for (Attachment.Template.Builder attachment : attachments) {
                    this.attachments.add(attachment.build());
                }
                return this;
            }

            public Builder setDynamicAttachments(DynamicAttachments dynamicAttachments) {
                this.dynamicAttachments = dynamicAttachments;
                return this;
            }

            public Template build() {
                TextTemplate[] to = this.to.isEmpty() ? null : this.to.toArray(new TextTemplate[this.to.size()]);
                Attachment.Template[] attachments = this.attachments.isEmpty()
                    ? null
                    : this.attachments.toArray(new Attachment.Template[this.attachments.size()]);
                return new Template(from, to, text, icon, attachments, dynamicAttachments);
            }
        }
    }

    interface XField extends MessageElement.XField {
        ParseField FROM = new ParseField("from");
        ParseField TO = new ParseField("to");
        ParseField ICON = new ParseField("icon");
        ParseField ATTACHMENTS = new ParseField("attachments");
        ParseField DYNAMIC_ATTACHMENTS = new ParseField("dynamic_attachments");
    }
}
