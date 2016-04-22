/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack.message;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
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

    public Attachment(String fallback, String color, String pretext, String authorName, String authorLink,
                      String authorIcon, String title, String titleLink, String text, Field[] fields,
                      String imageUrl, String thumbUrl) {

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
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Attachment that = (Attachment) o;

        if (fallback != null ? !fallback.equals(that.fallback) : that.fallback != null) return false;
        if (color != null ? !color.equals(that.color) : that.color != null) return false;
        if (pretext != null ? !pretext.equals(that.pretext) : that.pretext != null) return false;
        if (authorName != null ? !authorName.equals(that.authorName) : that.authorName != null) return false;
        if (authorLink != null ? !authorLink.equals(that.authorLink) : that.authorLink != null) return false;
        if (authorIcon != null ? !authorIcon.equals(that.authorIcon) : that.authorIcon != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (titleLink != null ? !titleLink.equals(that.titleLink) : that.titleLink != null) return false;
        if (text != null ? !text.equals(that.text) : that.text != null) return false;
        if (!Arrays.equals(fields, that.fields)) return false;
        if (imageUrl != null ? !imageUrl.equals(that.imageUrl) : that.imageUrl != null) return false;
        return !(thumbUrl != null ? !thumbUrl.equals(that.thumbUrl) : that.thumbUrl != null);
    }

    @Override
    public int hashCode() {
        int result = fallback != null ? fallback.hashCode() : 0;
        result = 31 * result + (color != null ? color.hashCode() : 0);
        result = 31 * result + (pretext != null ? pretext.hashCode() : 0);
        result = 31 * result + (authorName != null ? authorName.hashCode() : 0);
        result = 31 * result + (authorLink != null ? authorLink.hashCode() : 0);
        result = 31 * result + (authorIcon != null ? authorIcon.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (titleLink != null ? titleLink.hashCode() : 0);
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (fields != null ? Arrays.hashCode(fields) : 0);
        result = 31 * result + (imageUrl != null ? imageUrl.hashCode() : 0);
        result = 31 * result + (thumbUrl != null ? thumbUrl.hashCode() : 0);
        return result;
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

        return builder.endObject();
    }

    static class Template implements ToXContent {

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

        public Template(TextTemplate fallback, TextTemplate color, TextTemplate pretext, TextTemplate authorName,
                        TextTemplate authorLink, TextTemplate authorIcon, TextTemplate title, TextTemplate titleLink,
                        TextTemplate text, Field.Template[] fields, TextTemplate imageUrl, TextTemplate thumbUrl) {

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
            return new Attachment(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields, imageUrl,
                    thumbUrl);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;

            if (fallback != null ? !fallback.equals(template.fallback) : template.fallback != null) return false;
            if (color != null ? !color.equals(template.color) : template.color != null) return false;
            if (pretext != null ? !pretext.equals(template.pretext) : template.pretext != null) return false;
            if (authorName != null ? !authorName.equals(template.authorName) : template.authorName != null)
                return false;
            if (authorLink != null ? !authorLink.equals(template.authorLink) : template.authorLink != null)
                return false;
            if (authorIcon != null ? !authorIcon.equals(template.authorIcon) : template.authorIcon != null)
                return false;
            if (title != null ? !title.equals(template.title) : template.title != null) return false;
            if (titleLink != null ? !titleLink.equals(template.titleLink) : template.titleLink != null)
                return false;
            if (text != null ? !text.equals(template.text) : template.text != null) return false;
            if (!Arrays.equals(fields, template.fields)) return false;
            if (imageUrl != null ? !imageUrl.equals(template.imageUrl) : template.imageUrl != null) return false;
            return !(thumbUrl != null ? !thumbUrl.equals(template.thumbUrl) : template.thumbUrl != null);
        }

        @Override
        public int hashCode() {
            int result = fallback != null ? fallback.hashCode() : 0;
            result = 31 * result + (color != null ? color.hashCode() : 0);
            result = 31 * result + (pretext != null ? pretext.hashCode() : 0);
            result = 31 * result + (authorName != null ? authorName.hashCode() : 0);
            result = 31 * result + (authorLink != null ? authorLink.hashCode() : 0);
            result = 31 * result + (authorIcon != null ? authorIcon.hashCode() : 0);
            result = 31 * result + (title != null ? title.hashCode() : 0);
            result = 31 * result + (titleLink != null ? titleLink.hashCode() : 0);
            result = 31 * result + (text != null ? text.hashCode() : 0);
            result = 31 * result + (fields != null ? Arrays.hashCode(fields) : 0);
            result = 31 * result + (imageUrl != null ? imageUrl.hashCode() : 0);
            result = 31 * result + (thumbUrl != null ? thumbUrl.hashCode() : 0);
            return result;
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

            XContentParser.Token token = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.FALLBACK)) {
                    try {
                        fallback = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.FALLBACK);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.COLOR)) {
                    try {
                        color = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.COLOR);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.PRETEXT)) {
                    try {
                        pretext = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.PRETEXT);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.AUTHOR_NAME)) {
                    try {
                        authorName = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_NAME);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.AUTHOR_LINK)) {
                    try {
                        authorLink = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_LINK);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.AUTHOR_ICON)) {
                    try {
                        authorIcon = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.AUTHOR_ICON);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.TITLE)) {
                    try {
                        title = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TITLE);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.TITLE_LINK)) {
                    try {
                        titleLink = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TITLE_LINK);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.TEXT)) {
                    try {
                        text = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.TEXT);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.FIELDS)) {
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
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.IMAGE_URL)) {
                    try {
                        imageUrl = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.IMAGE_URL);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.THUMB_URL)) {
                    try {
                        thumbUrl = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment. failed to parse [{}] field", pe,
                                XField.THUMB_URL);
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
                    thumbUrl);
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

            private Builder() {
            }

            public Builder setFallback(TextTemplate fallback) {
                this.fallback = fallback;
                return this;
            }

            public Builder setFallback(TextTemplate.Builder fallback) {
                return setFallback(fallback.build());
            }

            public Builder setFallback(String fallback) {
                return setFallback(TextTemplate.indexed(fallback));
            }

            public Builder setColor(TextTemplate color) {
                this.color = color;
                return this;
            }

            public Builder setColor(TextTemplate.Builder color) {
                return setColor(color.build());
            }

            public Builder setColor(String color) {
                return setColor(TextTemplate.inline(color));
            }

            public Builder setPretext(TextTemplate pretext) {
                this.pretext = pretext;
                return this;
            }

            public Builder setPretext(TextTemplate.Builder pretext) {
                return setPretext(pretext.build());
            }

            public Builder setPretext(String pretext) {
                return setPretext(TextTemplate.inline(pretext));
            }

            public Builder setAuthorName(TextTemplate authorName) {
                this.authorName = authorName;
                return this;
            }

            public Builder setAuthorName(TextTemplate.Builder authorName) {
                return setAuthorName(authorName.build());
            }

            public Builder setAuthorName(String authorName) {
                return setAuthorName(TextTemplate.inline(authorName));
            }

            public Builder setAuthorLink(TextTemplate authorLink) {
                this.authorLink = authorLink;
                return this;
            }

            public Builder setAuthorLink(TextTemplate.Builder authorLink) {
                return setAuthorLink(authorLink.build());
            }

            public Builder setAuthorLink(String authorLink) {
                return setAuthorLink(TextTemplate.inline(authorLink));
            }

            public Builder setAuthorIcon(TextTemplate authorIcon) {
                this.authorIcon = authorIcon;
                return this;
            }

            public Builder setAuthorIcon(TextTemplate.Builder authorIcon) {
                return setAuthorIcon(authorIcon.build());
            }

            public Builder setAuthorIcon(String authorIcon) {
                return setAuthorIcon(TextTemplate.inline(authorIcon));
            }

            public Builder setTitle(TextTemplate title) {
                this.title = title;
                return this;
            }

            public Builder setTitle(TextTemplate.Builder title) {
                return setTitle(title.build());
            }

            public Builder setTitle(String title) {
                return setTitle(TextTemplate.inline(title));
            }

            public Builder setTitleLink(TextTemplate titleLink) {
                this.titleLink = titleLink;
                return this;
            }

            public Builder setTitleLink(TextTemplate.Builder titleLink) {
                return setTitleLink(titleLink.build());
            }

            public Builder setTitleLink(String titleLink) {
                return setTitleLink(TextTemplate.inline(titleLink));
            }

            public Builder setText(TextTemplate text) {
                this.text = text;
                return this;
            }

            public Builder setText(TextTemplate.Builder text) {
                return setText(text.build());
            }

            public Builder setText(String text) {
                return setText(TextTemplate.inline(text));
            }

            public Builder addField(TextTemplate title, TextTemplate value, boolean isShort) {
                fields.add(new Field.Template(title, value, isShort));
                return this;
            }

            public Builder addField(TextTemplate.Builder title, TextTemplate.Builder value, boolean isShort) {
                return addField(title.build(), value.build(), isShort);
            }

            public Builder addField(String title, String value, boolean isShort) {
                return addField(TextTemplate.inline(title), TextTemplate.inline(value), isShort);
            }

            public Builder setImageUrl(TextTemplate imageUrl) {
                this.imageUrl = imageUrl;
                return this;
            }

            public Builder setImageUrl(TextTemplate.Builder imageUrl) {
                return setImageUrl(imageUrl.build());
            }

            public Builder setImageUrl(String imageUrl) {
                return setImageUrl(TextTemplate.inline(imageUrl));
            }

            public Builder setThumbUrl(TextTemplate thumbUrl) {
                this.thumbUrl = thumbUrl;
                return this;
            }

            public Builder setThumbUrl(TextTemplate.Builder thumbUrl) {
                return setThumbUrl(thumbUrl.build());
            }

            public Builder setThumbUrl(String thumbUrl) {
                return setThumbUrl(TextTemplate.inline(thumbUrl));
            }

            public Template build() {
                Field.Template[] fields = this.fields.isEmpty() ? null : this.fields.toArray(new Field.Template[this.fields.size()]);
                return new Template(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, fields,
                        imageUrl, thumbUrl);
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
    }
}
