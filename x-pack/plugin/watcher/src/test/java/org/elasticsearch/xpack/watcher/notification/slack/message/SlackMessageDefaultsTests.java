/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class SlackMessageDefaultsTests extends ESTestCase {

    public void testConstructor() throws Exception {
        Settings settings = randomSettings();
        SlackMessageDefaults defaults = new SlackMessageDefaults(settings);
        assertThat(defaults.from, is(settings.get("from", null)));
        List<String> to = settings.getAsList("to", null);
        assertThat(defaults.to, is(to == null ? null : to.toArray(Strings.EMPTY_ARRAY)));
        assertThat(defaults.text, is(settings.get("text", null)));
        assertThat(defaults.icon, is(settings.get("icon", null)));
        assertThat(defaults.attachment.fallback, is(settings.get("attachment.fallback", null)));
        assertThat(defaults.attachment.color, is(settings.get("attachment.color", null)));
        assertThat(defaults.attachment.pretext, is(settings.get("attachment.pretext", null)));
        assertThat(defaults.attachment.authorName, is(settings.get("attachment.author_name", null)));
        assertThat(defaults.attachment.authorLink, is(settings.get("attachment.author_link", null)));
        assertThat(defaults.attachment.authorIcon, is(settings.get("attachment.author_icon", null)));
        assertThat(defaults.attachment.title, is(settings.get("attachment.title", null)));
        assertThat(defaults.attachment.titleLink, is(settings.get("attachment.title_link", null)));
        assertThat(defaults.attachment.text, is(settings.get("attachment.text", null)));
        assertThat(defaults.attachment.imageUrl, is(settings.get("attachment.image_url", null)));
        assertThat(defaults.attachment.thumbUrl, is(settings.get("attachment.thumb_url", null)));
        assertThat(defaults.attachment.field.title, is(settings.get("attachment.field.title", null)));
        assertThat(defaults.attachment.field.value, is(settings.get("attachment.field.value", null)));
        assertThat(defaults.attachment.field.isShort, is(settings.getAsBoolean("attachment.field.short", null)));
        assertThat(defaults.attachment.markdownSupportedFields, is(settings.getAsList("attachment.mrkdwn_in", null)));
    }

    public static Settings randomSettings() {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("from", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            String[] to = new String[randomIntBetween(1, 3)];
            for (int i = 0; i < to.length; i++) {
                to[i] = randomAlphaOfLength(10);
            }
            settings.putList("to", to);
        }
        if (randomBoolean()) {
            settings.put("text", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("icon", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.fallback", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.color", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.pretext", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.author_name", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.author_link", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.author_icon", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.title", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.title_link", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.text", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.image_url", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.thumb_url", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.field.title", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.field.value", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            settings.put("attachment.field.short", randomBoolean());
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                settings.putList("attachment.mrkdwn_in", "foo", "bar");
            } else {
                settings.put("attachment.mrkdwn_in", "foo,bar");
            }
        }
        return settings.build();
    }
}
