/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Attachment implements MessageElement {

    final String fallback;
    final String color;
    final String pretext;
    final String authorName;
    final String authorLink;
    final String authorIcon;
    final String title;
    final String titleLink;
    final String text;
    final Field[] fields;
    final String imageUrl;
    final String thumbUrl;
    final String[] markdownSupportedFields;
    final List<Action> actions;

    public Attachment(String fallback, String color, String pretext, String authorName, String authorLink,
                      String authorIcon, String title, String titleLink, String text, Field[] fields,
                      String imageUrl, String thumbUrl, String[] markdownSupportedFields, List<Action> actions) {

        this.fallback = fallback;
        this.color = color;
        this.pretext = pretext;
        this.authorName = authorName;
        this.authorLink = authorLink;
        this.authorIcon = authorIcon;
        this.title = title;
        this.titleLink = titleLink;
        this.text = text;
        this.fields = fields;
        this.imageUrl = imageUrl;
        this.thumbUrl = thumbUrl;
        this.markdownSupportedFields = markdownSupportedFields;
        this.actions = actions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Attachment that = (Attachment) o;
        return Objects.equals(fallback, that.fallback) && Objects.equals(color, that.color) &&
                Objects.equals(pretext, that.pretext) && Objects.equals(authorName, that.authorName) &&
                Objects.equals(authorLink, that.authorLink) && Objects.equals(authorIcon, that.authorIcon) &&
                Objects.equals(title, that.title) && Objects.equals(titleLink, that.titleLink) &&
                Objects.equals(text, that.text) && Objects.equals(imageUrl, that.imageUrl) &&
                Objects.equals(thumbUrl, that.thumbUrl) &&  Objects.equals(actions, that.actions) &&
                Arrays.equals(markdownSupportedFields, that.markdownSupportedFields) && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                            thumbUrl, markdownSupportedFields, actions);
    }

    /**
     * renders the attachment in slack compatible structure:
     * <p>
     * https://api.slack.com/docs/attachments#attachment_structure
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (fallback != null) {
            builder.field(XField.FALLBACK.getPreferredName(), fallback);
        }
        if (color != null) {
            builder.field(XField.COLOR.getPreferredName(), color);
        }
        if (pretext != null) {
            builder.field(XField.PRETEXT.getPreferredName(), pretext);
        }
        if (authorName != null) {
            builder.field(XField.AUTHOR_NAME.getPreferredName(), authorName);
            if (authorLink != null) {
                builder.field(XField.AUTHOR_LINK.getPreferredName(), authorLink);
            }
            if (authorIcon != null) {
                builder.field(XField.AUTHOR_ICON.getPreferredName(), authorIcon);
            }
        }
        if (title != null) {
            builder.field(XField.TITLE.getPreferredName(), title);
            if (titleLink != null) {
                builder.field(XField.TITLE_LINK.getPreferredName(), titleLink);
            }
        }
        if (text != null) {
            builder.field(XField.TEXT.getPreferredName(), text);
        }
        if (fields != null) {
            builder.startArray(XField.FIELDS.getPreferredName());
            for (Field field : fields) {
                field.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (imageUrl != null) {
            builder.field(XField.IMAGE_URL.getPreferredName(), imageUrl);
        }
        if (thumbUrl != null) {
            builder.field(XField.THUMB_URL.getPreferredName(), thumbUrl);
        }
        if (markdownSupportedFields != null) {
            builder.startArray(XField.MARKDOWN_IN.getPreferredName());
            for (String field : markdownSupportedFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (actions != null && actions.isEmpty() == false) {
            builder.startArray("actions");
            for (Action action : actions) {
                action.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    static class Template implements ToXContentObject {

        final TextTemplate fallback;
        final TextTemplate color;
        final TextTemplate pretext;
        final TextTemplate authorName;
        final TextTemplate authorLink;
        final TextTemplate authorIcon;
        final TextTemplate title;
        final TextTemplate titleLink;
        final TextTemplate text;
        final Field.Template[] fields;
        final TextTemplate imageUrl;
        final TextTemplate thumbUrl;
        final TextTemplate[] markdownSupportedFields;
        final List<Action.Template> actions;

        Template(TextTemplate fallback, TextTemplate color, TextTemplate pretext, TextTemplate authorName,
                 TextTemplate authorLink, TextTemplate authorIcon, TextTemplate title, TextTemplate titleLink,
                 TextTemplate text, Field.Template[] fields, TextTemplate imageUrl, TextTemplate thumbUrl,
                 TextTemplate[] markdownSupportedFields, List<Action.Template> actions) {

            this.fallback = fallback;
            this.color = color;
            this.pretext = pretext;
            this.authorName = authorName;
            this.authorLink = authorLink;
            this.authorIcon = authorIcon;
            this.title = title;
            this.titleLink = titleLink;
            this.text = text;
            this.fields = fields;
            this.imageUrl = imageUrl;
            this.thumbUrl = thumbUrl;
            this.markdownSupportedFields = markdownSupportedFields;
            this.actions = actions;
        }

        public Attachment render(TextTemplateEngine engine, Map<String, Object> model, SlackMessageDefaults.AttachmentDefaults defaults) {
            String fallback = this.fallback != null ? engine.render(this.fallback, model) : defaults.fallback;
            String color = this.color != null ? engine.render(this.color, model) : defaults.color;
            String pretext = this.pretext != null ? engine.render(this.pretext, model) : defaults.pretext;
            String authorName = this.authorName != null ? engine.render(this.authorName, model) : defaults.authorName;
            String authorLink = this.authorLink != null ? engine.render(this.authorLink, model) : defaults.authorLink;
            String authorIcon = this.authorIcon != null ? engine.render(this.authorIcon, model) : defaults.authorIcon;
            String title = this.title != null ? engine.render(this.title, model) : defaults.title;
            String titleLink = this.titleLink != null ? engine.render(this.titleLink, model) : defaults.titleLink;
            String text = this.text != null ? engine.render(this.text, model) : defaults.text;
            String imageUrl = this.imageUrl != null ? engine.render(this.imageUrl, model) : defaults.imageUrl;
            String thumbUrl = this.thumbUrl != null ? engine.render(this.thumbUrl, model) : defaults.thumbUrl;
            Field[] fields = null;
            if (this.fields != null) {
                fields = new Field[this.fields.length];
                for (int i = 0; i < fields.length; i++) {
                    fields[i] = this.fields[i].render(engine, model, defaults.field);
                }
            }
            String[] markdownFields = null;
            if (this.markdownSupportedFields != null) {
                markdownFields = new String[this.markdownSupportedFields.length];
                for (int i = 0; i < markdownSupportedFields.length; i++) {
                    markdownFields[i] = engine.render(this.markdownSupportedFields[i], model);
                }
            }
            List<Action> actions = new ArrayList<>();
            if (this.actions != null && this.actions.isEmpty() == false) {
                for (Action.Template action : this.actions) {
                    actions.add(action.render(engine, model));
                }
            }

            return new Attachment(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                    thumbUrl, markdownFields, actions);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;

            return Objects.equals(fallback, template.fallback) && Objects.equals(color, template.color) &&
                    Objects.equals(pretext, template.pretext) && Objects.equals(authorName, template.authorName) &&
                    Objects.equals(authorLink, template.authorLink) && Objects.equals(authorIcon, template.authorIcon) &&
                    Objects.equals(title, template.title) && Objects.equals(titleLink, template.titleLink) &&
                    Objects.equals(text, template.text) && Objects.equals(imageUrl, template.imageUrl) &&
                    Objects.equals(thumbUrl, template.thumbUrl) && Objects.equals(actions, template.actions) &&
                    Arrays.equals(fields, template.fields) &&
                    Arrays.equals(markdownSupportedFields, template.markdownSupportedFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                    thumbUrl, markdownSupportedFields, actions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (fallback != null) {
                builder.field(XField.FALLBACK.getPreferredName(), fallback, params);
            }
            if (color != null) {
                builder.field(XField.COLOR.getPreferredName(), color, params);
            }
            if (pretext != null) {
                builder.field(XField.PRETEXT.getPreferredName(), pretext, params);
            }
            if (authorName != null) {
                builder.field(XField.AUTHOR_NAME.getPreferredName(), authorName, params);
                if (authorLink != null) {
                    builder.field(XField.AUTHOR_LINK.getPreferredName(), authorLink, params);
                }
                if (authorIcon != null) {
                    builder.field(XField.AUTHOR_ICON.getPreferredName(), authorIcon, params);
                }
            }
            if (title != null) {
                builder.field(XField.TITLE.getPreferredName(), title, params);
                if (titleLink != null) {
                    builder.field(XField.TITLE_LINK.getPreferredName(), titleLink, params);
                }
            }
            if (text != null) {
                builder.field(XField.TEXT.getPreferredName(), text, params);
            }
            if (fields != null) {
                builder.startArray(XField.FIELDS.getPreferredName());
                for (Field.Template field : fields) {
                    field.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (imageUrl != null) {
                builder.field(XField.IMAGE_URL.getPreferredName(), imageUrl, params);
            }
            if (thumbUrl != null) {
                builder.field(XField.THUMB_URL.getPreferredName(), thumbUrl, params);
            }
            if (markdownSupportedFields != null) {
                builder.startArray(XField.MARKDOWN_IN.getPreferredName());
                for (TextTemplate field : markdownSupportedFields) {
                    field.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (actions != null && actions.isEmpty() == false) {
                builder.startArray(XField.ACTIONS.getPreferredName());
                for (Action.Template action : actions) {
                    action.toXContent(builder, params);
                }
                builder.endArray();
            }

            return builder.endObject();
        }

        public static Template parse(XContentParser parser) throws IOException {

            TextTemplate fallback = null;
            TextTemplate color = null;
            TextTemplate pretext = null;
            TextTemplate authorName = null;
            TextTemplate authorLink = null;
            TextTemplate authorIcon = null;
            TextTemplate title = null;
            TextTemplate titleLink = null;
            TextTemplate text = null;
            Field.Template[] fields = null;
            TextTemplate imageUrl = null;
            TextTemplate thumbUrl = null;
            TextTemplate[] markdownFields = null;
            List<Action.Template> actions = new ArrayList<>();

            XContentParser.Token token = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (XField.FALLBACK.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        fallback = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.FALLBACK);
                    }
                } else if (XField.COLOR.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        color = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.COLOR);
                    }
                } else if (XField.PRETEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        pretext = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.PRETEXT);
                    }
                } else if (XField.AUTHOR_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        authorName = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_NAME);
                    }
                } else if (XField.AUTHOR_LINK.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        authorLink = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_LINK);
                    }
                } else if (XField.AUTHOR_ICON.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        authorIcon = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_ICON);
                    }
                } else if (XField.TITLE.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        title = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TITLE);
                    }
                } else if (XField.TITLE_LINK.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        titleLink = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TITLE_LINK);
                    }
                } else if (XField.TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        text = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TEXT);
                    }
                } else if (XField.FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        List<Field.Template> list = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                list.add(Field.Template.parse(parser));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field",
                                        pe, XField.FIELDS);
                            }
                        }
                        fields = list.toArray(new Field.Template[list.size()]);
                    } else {
                        try {
                            fields = new Field.Template[]{Field.Template.parse(parser)};
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                    XField.FIELDS);
                        }
                    }
                } else if (XField.IMAGE_URL.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        imageUrl = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.IMAGE_URL);
                    }
                } else if (XField.THUMB_URL.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        thumbUrl = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.THUMB_URL);
                    }
                } else if (XField.MARKDOWN_IN.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        List<TextTemplate> list = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                list.add(new TextTemplate(parser.text()));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field",
                                        pe, XField.MARKDOWN_IN);
                            }
                        }
                        markdownFields = list.toArray(new TextTemplate[list.size()]);
                    } else {
                        try {
                            markdownFields = new TextTemplate[]{new TextTemplate(parser.text())};
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                    XField.MARKDOWN_IN);
                        }
                    }
                } else if (XField.ACTIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        actions.add(Action.ACTION_PARSER.parse(parser, null));
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse message attachment field. unexpected field [{}]",
                            currentFieldName);
                }
            }
            if (authorName == null) {
                if (authorLink != null) {
                    throw new ElasticsearchParseException("could not parse message attachment field. found field [{}], but no [{}] is " +
                            "defined", XField.AUTHOR_LINK, XField.AUTHOR_NAME);
                }
                if (authorIcon != null) {
                    throw new ElasticsearchParseException("could not parse message attachment field. found field [{}], but no [{}] is " +
                            "defined", XField.AUTHOR_ICON, XField.AUTHOR_NAME);
                }
            }
            if (title == null) {
                if (titleLink != null) {
                    throw new ElasticsearchParseException("could not parse message attachment field. found field [{}], but no [{}] is " +
                            "defined", XField.TITLE_LINK, XField.TITLE);
                }
            }
            return new Template(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                    thumbUrl, markdownFields, actions);
        }


        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private TextTemplate fallback;
            private TextTemplate color;
            private TextTemplate pretext;
            private TextTemplate authorName;
            private TextTemplate authorLink;
            private TextTemplate authorIcon;
            private TextTemplate title;
            private TextTemplate titleLink;
            private TextTemplate text;
            private List<Field.Template> fields = new ArrayList<>();
            private TextTemplate imageUrl;
            private TextTemplate thumbUrl;
            private List<TextTemplate> markdownFields = new ArrayList<>();
            private List<Action.Template> actions = new ArrayList<>();

            private Builder() {
            }

            public Builder setFallback(TextTemplate fallback) {
                this.fallback = fallback;
                return this;
            }

            public Builder setFallback(String fallback) {
                return setFallback(new TextTemplate(fallback));
            }

            public Builder setColor(TextTemplate color) {
                this.color = color;
                return this;
            }

            public Builder setColor(String color) {
                return setColor(new TextTemplate(color));
            }

            public Builder setPretext(TextTemplate pretext) {
                this.pretext = pretext;
                return this;
            }

            public Builder setPretext(String pretext) {
                return setPretext(new TextTemplate(pretext));
            }

            public Builder setAuthorName(TextTemplate authorName) {
                this.authorName = authorName;
                return this;
            }

            public Builder setAuthorName(String authorName) {
                return setAuthorName(new TextTemplate(authorName));
            }

            public Builder setAuthorLink(TextTemplate authorLink) {
                this.authorLink = authorLink;
                return this;
            }

            public Builder setAuthorLink(String authorLink) {
                return setAuthorLink(new TextTemplate(authorLink));
            }

            public Builder setAuthorIcon(TextTemplate authorIcon) {
                this.authorIcon = authorIcon;
                return this;
            }

            public Builder setAuthorIcon(String authorIcon) {
                return setAuthorIcon(new TextTemplate(authorIcon));
            }

            public Builder setTitle(TextTemplate title) {
                this.title = title;
                return this;
            }

            public Builder setTitle(String title) {
                return setTitle(new TextTemplate(title));
            }

            public Builder setTitleLink(TextTemplate titleLink) {
                this.titleLink = titleLink;
                return this;
            }

            public Builder setTitleLink(String titleLink) {
                return setTitleLink(new TextTemplate(titleLink));
            }

            public Builder setText(TextTemplate text) {
                this.text = text;
                return this;
            }

            public Builder setText(String text) {
                return setText(new TextTemplate(text));
            }

            public Builder addField(TextTemplate title, TextTemplate value, boolean isShort) {
                fields.add(new Field.Template(title, value, isShort));
                return this;
            }

            public Builder addField(String title, String value, boolean isShort) {
                return addField(new TextTemplate(title), new TextTemplate(value), isShort);
            }

            public Builder setImageUrl(TextTemplate imageUrl) {
                this.imageUrl = imageUrl;
                return this;
            }

            public Builder setImageUrl(String imageUrl) {
                return setImageUrl(new TextTemplate(imageUrl));
            }

            public Builder setThumbUrl(TextTemplate thumbUrl) {
                this.thumbUrl = thumbUrl;
                return this;
            }

            public Builder setThumbUrl(String thumbUrl) {
                return setThumbUrl(new TextTemplate(thumbUrl));
            }

            public Builder addMarkdownField(String name) {
                this.markdownFields.add(new TextTemplate(name));
                return this;
            }

            public Builder addAction(Action.Template action) {
                this.actions.add(action);
                return this;
            }

            public Template build() {
                Field.Template[] fields = this.fields.isEmpty() ? null : this.fields.toArray(new Field.Template[this.fields.size()]);
                TextTemplate[] markdownFields =
                        this.markdownFields.isEmpty() ? null : this.markdownFields.toArray(new TextTemplate[this.markdownFields.size()]);
                return new Template(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                        thumbUrl, markdownFields, actions);
            }
        }
    }

    interface XField extends MessageElement.XField {
        ParseField FALLBACK = new ParseField("fallback");
        ParseField COLOR = new ParseField("color");
        ParseField PRETEXT = new ParseField("pretext");
        ParseField AUTHOR_NAME = new ParseField("author_name");
        ParseField AUTHOR_LINK = new ParseField("author_link");
        ParseField AUTHOR_ICON = new ParseField("author_icon");

        ParseField TITLE_LINK = new ParseField("title_link");
        ParseField FIELDS = new ParseField("fields");
        ParseField IMAGE_URL = new ParseField("image_url");
        ParseField THUMB_URL = new ParseField("thumb_url");

        ParseField MARKDOWN_IN = new ParseField("mrkdwn_in");
        ParseField ACTIONS = new ParseField("actions");
    }
}
