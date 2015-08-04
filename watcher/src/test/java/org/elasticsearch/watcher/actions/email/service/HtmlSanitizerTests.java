/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;


/**
 *
 */
public class HtmlSanitizerTests extends ESTestCase {

    @Test
    public void testDefault_WithTemplatePlaceholders() {
        String blockTag = randomFrom(HtmlSanitizer.BLOCK_TAGS);
        while (blockTag.equals("li")) {
            blockTag = randomFrom(HtmlSanitizer.BLOCK_TAGS);
        }
        String html =
                "<html>" +
                        "<head></head>" +
                        "<body>" +
                        "<" + blockTag + ">Hello {{ctx.metadata.name}}</" + blockTag + ">" +
                        "<ul><li>item1</li></ul>" +
                        "<ol><li>item2</li></ol>" +
                        "meta <a href='https://www.google.com/search?q={{ctx.metadata.name}}'>Testlink</a> meta" +
                        "</body>" +
                        "</html>";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(html);
        if (blockTag.equals("ol") || blockTag.equals("ul")) {
            assertThat(sanitizedHtml, equalTo(
                    "<head></head><body>" +
                            "<" + blockTag + "><li>Hello {{ctx.metadata.name}}</li></" + blockTag + ">" +
                            "<ul><li>item1</li></ul>" +
                            "<ol><li>item2</li></ol>" +
                            "meta <a href=\"https://www.google.com/search?q&#61;{{ctx.metadata.name}}\" rel=\"nofollow\">Testlink</a> meta" +
                            "</body>"));
        } else {
            assertThat(sanitizedHtml, equalTo(
                    "<head></head><body>" +
                            "<" + blockTag + ">Hello {{ctx.metadata.name}}</" + blockTag + ">" +
                            "<ul><li>item1</li></ul>" +
                            "<ol><li>item2</li></ol>" +
                            "meta <a href=\"https://www.google.com/search?q&#61;{{ctx.metadata.name}}\" rel=\"nofollow\">Testlink</a> meta" +
                            "</body>"));
        }
    }


    @Test
    public void testDefault_onclick_Disallowed() {
        String badHtml = "<button type=\"button\"" +
                "onclick=\"document.getElementById('demo').innerHTML = Date()\">" +
                "Click me to display Date and Time.</button>";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(badHtml);
        assertThat(sanitizedHtml, equalTo("Click me to display Date and Time."));
    }

    @Test
    public void testDefault_ExternalImage_Disallowed() {
        String html = "<img src=\"http://test.com/nastyimage.jpg\"/>This is a bad image";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo("This is a bad image"));
    }

    @Test
    public void testDefault_EmbeddedImage_Allowed() {
        String html = "<img src=\"cid:foo\" />This is a good image";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo(html));
    }

    @Test
    public void testDefault_Tables_Allowed() {
        String html = "<table><tr><td>cell1</td><td>cell2</td></tr></table>";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo(html));
    }

    @Test
    public void testDefault_Scipts_Disallowed() {
        String html = "<script>doSomethingNefarious()</script>This was a dangerous script";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.EMPTY);
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo("This was a dangerous script"));
    }

    @Test
    public void testCustom_Disabled() {
        String html = "<img src=\"http://test.com/nastyimage.jpg\" />This is a bad image";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.builder()
                .put("watcher.actions.email.html.sanitization.enabled", false)
                .build());
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo(html));
    }

    @Test
    public void testCustom_AllImage_Allowed() {
        String html = "<img src=\"http://test.com/nastyimage.jpg\" />This is a bad image";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.builder()
                .put("watcher.actions.email.html.sanitization.allow", "img:all")
                .build());
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo(html));
    }

    @Test
    public void testCustom_Tables_Disallowed() {
        String html = "<table><tr><td>cell1</td><td>cell2</td></tr></table>";
        HtmlSanitizer sanitizer = new HtmlSanitizer(Settings.builder()
                .put("watcher.actions.email.html.sanitization.disallow", "_tables")
                .build());
        String sanitizedHtml = sanitizer.sanitize(html);
        assertThat(sanitizedHtml, equalTo("cell1cell2"));
    }

}
