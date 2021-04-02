/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SlackMessageDefaults {

    private static final String FROM_SETTING = SlackMessage.XField.FROM.getPreferredName();
    private static final String TO_SETTING = SlackMessage.XField.TO.getPreferredName();
    private static final String ICON_SETTING = SlackMessage.XField.ICON.getPreferredName();
    private static final String TEXT_SETTING = SlackMessage.XField.TEXT.getPreferredName();
    private static final String ATTACHMENT_SETTING = "attachment";

    public final String from;
    public final String[] to;
    public final String icon;
    public final String text;
    public final AttachmentDefaults attachment;

    public SlackMessageDefaults(Settings settings) {
        from = settings.get(FROM_SETTING, null);
        List<String> to = settings.getAsList(TO_SETTING, null);
        this.to = to == null ? null : to.toArray(Strings.EMPTY_ARRAY);
        icon = settings.get(ICON_SETTING, null);
        text = settings.get(TEXT_SETTING, null);
        attachment = new AttachmentDefaults(settings.getAsSettings(ATTACHMENT_SETTING));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SlackMessageDefaults defaults = (SlackMessageDefaults) o;

        return Objects.equals(from, defaults.from)
            && Arrays.equals(to, defaults.to)
            && Objects.equals(icon, defaults.icon)
            && Objects.equals(text, defaults.text)
            && Objects.equals(attachment, defaults.attachment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(to, icon, text, attachment);
    }

    static class AttachmentDefaults {

        static final String FALLBACK_SETTING = Attachment.XField.FALLBACK.getPreferredName();
        static final String COLOR_SETTING = Attachment.XField.COLOR.getPreferredName();
        static final String PRETEXT_SETTING = Attachment.XField.PRETEXT.getPreferredName();
        static final String AUTHOR_NAME_SETTING = Attachment.XField.AUTHOR_NAME.getPreferredName();
        static final String AUTHOR_LINK_SETTING = Attachment.XField.AUTHOR_LINK.getPreferredName();
        static final String AUTHOR_ICON_SETTING = Attachment.XField.AUTHOR_ICON.getPreferredName();
        static final String TITLE_SETTING = Attachment.XField.TITLE.getPreferredName();
        static final String TITLE_LINK_SETTING = Attachment.XField.TITLE_LINK.getPreferredName();
        static final String TEXT_SETTING = Attachment.XField.TEXT.getPreferredName();
        static final String IMAGE_URL_SETTING = Attachment.XField.IMAGE_URL.getPreferredName();
        static final String THUMB_URL_SETTING = Attachment.XField.THUMB_URL.getPreferredName();
        static final String MARKDOWN_IN_SETTING = Attachment.XField.MARKDOWN_IN.getPreferredName();
        static final String FIELD_SETTING = "field";

        final String fallback;
        final String color;
        final String pretext;
        final String authorName;
        final String authorLink;
        final String authorIcon;
        final String title;
        final String titleLink;
        final String text;
        final String imageUrl;
        final String thumbUrl;
        final List<String> markdownSupportedFields;
        final FieldDefaults field;

        AttachmentDefaults(Settings settings) {
            fallback = settings.get(FALLBACK_SETTING, null);
            color = settings.get(COLOR_SETTING, null);
            pretext = settings.get(PRETEXT_SETTING, null);
            authorName = settings.get(AUTHOR_NAME_SETTING, null);
            authorLink = settings.get(AUTHOR_LINK_SETTING, null);
            authorIcon = settings.get(AUTHOR_ICON_SETTING, null);
            title = settings.get(TITLE_SETTING, null);
            titleLink = settings.get(TITLE_LINK_SETTING, null);
            text = settings.get(TEXT_SETTING, null);
            imageUrl = settings.get(IMAGE_URL_SETTING, null);
            thumbUrl = settings.get(THUMB_URL_SETTING, null);
            markdownSupportedFields = settings.getAsList(MARKDOWN_IN_SETTING, null);
            field = new FieldDefaults(settings.getAsSettings(FIELD_SETTING));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AttachmentDefaults that = (AttachmentDefaults) o;

            return Objects.equals(fallback, that.fallback) && Objects.equals(color, that.color) &&
                    Objects.equals(pretext, that.pretext) && Objects.equals(authorName, that.authorName) &&
                    Objects.equals(authorLink, that.authorLink) && Objects.equals(authorIcon, that.authorIcon) &&
                    Objects.equals(title, that.title) && Objects.equals(titleLink, that.titleLink) &&
                    Objects.equals(text, that.text) && Objects.equals(imageUrl, that.imageUrl) &&
                    Objects.equals(thumbUrl, that.thumbUrl) && Objects.equals(field, that.field) &&
                    Objects.equals(markdownSupportedFields, that.markdownSupportedFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink, text, imageUrl,
                    thumbUrl, field, markdownSupportedFields);
        }

        static class FieldDefaults {

            static final String TITLE_SETTING = Field.XField.TITLE.getPreferredName();
            static final String VALUE_SETTING = Field.XField.VALUE.getPreferredName();
            static final String SHORT_SETTING = Field.XField.SHORT.getPreferredName();

            final String title;
            final String value;
            final Boolean isShort;

            FieldDefaults(Settings settings) {
                title = settings.get(TITLE_SETTING, null);
                value = settings.get(VALUE_SETTING, null);
                isShort = settings.getAsBoolean(SHORT_SETTING, null);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                FieldDefaults that = (FieldDefaults) o;

                return Objects.equals(title, that.title) && Objects.equals(value, that.value) && Objects.equals(isShort, that.isShort);
            }

            @Override
            public int hashCode() {
                return Objects.hash(title, value, isShort);
            }
        }
    }
}
