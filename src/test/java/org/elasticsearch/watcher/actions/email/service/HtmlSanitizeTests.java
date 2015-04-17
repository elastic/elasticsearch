/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class HtmlSanitizeTests extends ElasticsearchTestCase {

    @Test
    public void test_HtmlSanitizer_onclick() {
        String badHtml = "<button type=\"button\"" +
                "onclick=\"document.getElementById('demo').innerHTML = Date()\">" +
                "Click me to display Date and Time.</button>";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), badHtml);
        assertThat(sanitizedHtml, equalTo("Click me to display Date and Time."));
    }

    @Test
    public void test_HtmlSanitizer_Nonattachment_img() {
        String badHtml = "<img src=\"http://test.com/nastyimage.jpg\"/>This is a bad image";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), badHtml);
        assertThat(sanitizedHtml, equalTo("This is a bad image"));
    }

    @Test
    public void test_HtmlSanitizer_Goodattachment_img() {
        String goodHtml = "<img src=\"cid:foo\" />This is a good image";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), goodHtml);
        assertThat(sanitizedHtml, equalTo(goodHtml));
    }

    @Test
    public void test_HtmlSanitizer_table() {
        String goodHtml = "<table><tr><td>cell1</td><td>cell2</td></tr></table>";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), goodHtml);
        assertThat(sanitizedHtml, equalTo(goodHtml));

    }

    @Test
    public void test_HtmlSanitizer_Badattachment_img() {
        String goodHtml = "<img src=\"cid:bad\" />This is a bad image";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), goodHtml);
        assertThat(sanitizedHtml, equalTo("This is a bad image"));
    }

    @Test
    public void test_HtmlSanitizer_Script() {
        String badHtml = "<script>doSomethingNefarious()</script>This was a dangerous script";
        byte[] bytes = new byte[0];
        String sanitizedHtml = Profile.sanitizeHtml(ImmutableMap.of("foo", (Attachment) new Attachment.Bytes("foo", bytes, "")), badHtml);
        assertThat(sanitizedHtml, equalTo("This was a dangerous script"));
    }


}
