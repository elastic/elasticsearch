/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack.message;

import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;

/**
 *
 */
public class SlackMessageDefaults {

    public static final String FROM_SETTING = SlackMessage.XField.FROM.getPreferredName();
    public static final String TO_SETTING = SlackMessage.XField.TO.getPreferredName();
    public static final String ICON_SETTING = SlackMessage.XField.ICON.getPreferredName();
    public static final String TEXT_SETTING = SlackMessage.XField.TEXT.getPreferredName();
    public static final String ATTACHMENT_SETTING = "attachment";

    public final String from;
    public final String[] to;
    public final String icon;
    public final String text;
    public final AttachmentDefaults attachment;

    public SlackMessageDefaults(Settings settings) {
        from = settings.get(FROM_SETTING, null);
        to = settings.getAsArray(TO_SETTING, null);
        icon = settings.get(ICON_SETTING, null);
        text = settings.get(TEXT_SETTING, null);
        attachment = new AttachmentDefaults(settings.getAsSettings(ATTACHMENT_SETTING));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SlackMessageDefaults defaults = (SlackMessageDefaults) o;

        if (from != null ? !from.equals(defaults.from) : defaults.from != null) return false;
        if (!Arrays.equals(to, defaults.to)) return false;
        if (icon != null ? !icon.equals(defaults.icon) : defaults.icon != null) return false;
        if (text != null ? !text.equals(defaults.text) : defaults.text != null) return false;
        return !(attachment != null ? !attachment.equals(defaults.attachment) : defaults.attachment != null);
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? Arrays.hashCode(to) : 0);
        result = 31 * result + (icon != null ? icon.hashCode() : 0);
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (attachment != null ? attachment.hashCode() : 0);
        return result;
    }

    static class AttachmentDefaults {

        public static final String FALLBACK_SETTING = Attachment.XField.FALLBACK.getPreferredName();
        public static final String COLOR_SETTING = Attachment.XField.COLOR.getPreferredName();
        public static final String PRETEXT_SETTING = Attachment.XField.PRETEXT.getPreferredName();
        public static final String AUTHOR_NAME_SETTING = Attachment.XField.AUTHOR_NAME.getPreferredName();
        public static final String AUTHOR_LINK_SETTING = Attachment.XField.AUTHOR_LINK.getPreferredName();
        public static final String AUTHOR_ICON_SETTING = Attachment.XField.AUTHOR_ICON.getPreferredName();
        public static final String TITLE_SETTING = Attachment.XField.TITLE.getPreferredName();
        public static final String TITLE_LINK_SETTING = Attachment.XField.TITLE_LINK.getPreferredName();
        public static final String TEXT_SETTING = Attachment.XField.TEXT.getPreferredName();
        public static final String IMAGE_URL_SETTING = Attachment.XField.IMAGE_URL.getPreferredName();
        public static final String THUMB_URL_SETTING = Attachment.XField.THUMB_URL.getPreferredName();
        public static final String FIELD_SETTING = "field";

        public final String fallback;
        public final String color;
        public final String pretext;
        public final String authorName;
        public final String authorLink;
        public final String authorIcon;
        public final String title;
        public final String titleLink;
        public final String text;
        public final String imageUrl;
        public final String thumbUrl;
        public final FieldDefaults field;

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
            field = new FieldDefaults(settings.getAsSettings(FIELD_SETTING));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AttachmentDefaults that = (AttachmentDefaults) o;

            if (fallback != null ? !fallback.equals(that.fallback) : that.fallback != null) return false;
            if (color != null ? !color.equals(that.color) : that.color != null) return false;
            if (pretext != null ? !pretext.equals(that.pretext) : that.pretext != null) return false;
            if (authorName != null ? !authorName.equals(that.authorName) : that.authorName != null) return false;
            if (authorLink != null ? !authorLink.equals(that.authorLink) : that.authorLink != null) return false;
            if (authorIcon != null ? !authorIcon.equals(that.authorIcon) : that.authorIcon != null) return false;
            if (title != null ? !title.equals(that.title) : that.title != null) return false;
            if (titleLink != null ? !titleLink.equals(that.titleLink) : that.titleLink != null) return false;
            if (text != null ? !text.equals(that.text) : that.text != null) return false;
            if (imageUrl != null ? !imageUrl.equals(that.imageUrl) : that.imageUrl != null) return false;
            if (thumbUrl != null ? !thumbUrl.equals(that.thumbUrl) : that.thumbUrl != null) return false;
            return !(field != null ? !field.equals(that.field) : that.field != null);
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
            result = 31 * result + (imageUrl != null ? imageUrl.hashCode() : 0);
            result = 31 * result + (thumbUrl != null ? thumbUrl.hashCode() : 0);
            result = 31 * result + (field != null ? field.hashCode() : 0);
            return result;
        }

        static class FieldDefaults {

            public static final String TITLE_SETTING = Field.XField.TITLE.getPreferredName();
            public static final String VALUE_SETTING = Field.XField.VALUE.getPreferredName();
            public static final String SHORT_SETTING = Field.XField.SHORT.getPreferredName();

            public final String title;
            public final String value;
            public final Boolean isShort;

            public FieldDefaults(Settings settings) {
                title = settings.get(TITLE_SETTING, null);
                value = settings.get(VALUE_SETTING, null);
                isShort = settings.getAsBoolean(SHORT_SETTING, null);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                FieldDefaults that = (FieldDefaults) o;

                if (title != null ? !title.equals(that.title) : that.title != null) return false;
                if (value != null ? !value.equals(that.value) : that.value != null) return false;
                return !(isShort != null ? !isShort.equals(that.isShort) : that.isShort != null);
            }

            @Override
            public int hashCode() {
                int result = title != null ? title.hashCode() : 0;
                result = 31 * result + (value != null ? value.hashCode() : 0);
                result = 31 * result + (isShort != null ? isShort.hashCode() : 0);
                return result;
            }
        }
    }
}
