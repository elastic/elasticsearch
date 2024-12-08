/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EmailTemplateTests extends ESTestCase {

    public void testEmailTemplateParserSelfGenerated() throws Exception {
        TextTemplate from = randomFrom(new TextTemplate("from@from.com"), null);
        List<TextTemplate> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            addresses.add(new TextTemplate("address" + i + "@test.com"));
        }
        TextTemplate[] possibleList = addresses.toArray(new TextTemplate[addresses.size()]);
        TextTemplate[] replyTo = randomFrom(possibleList, null);
        TextTemplate[] to = randomFrom(possibleList, null);
        TextTemplate[] cc = randomFrom(possibleList, null);
        TextTemplate[] bcc = randomFrom(possibleList, null);
        TextTemplate priority = new TextTemplate(randomFrom(Email.Priority.values()).name());

        TextTemplate subjectTemplate = new TextTemplate("Templated Subject {{foo}}");
        TextTemplate textBodyTemplate = new TextTemplate("Templated Body {{foo}}");

        TextTemplate htmlBodyTemplate = new TextTemplate("Templated Html Body <script>nefarious scripting</script>");
        String htmlBody = "Templated Html Body <script>nefarious scripting</script>";
        String sanitizedHtmlBody = "Templated Html Body";

        EmailTemplate emailTemplate = new EmailTemplate(
            from,
            replyTo,
            priority,
            to,
            cc,
            bcc,
            subjectTemplate,
            textBodyTemplate,
            htmlBodyTemplate
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        emailTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        parser.nextToken();

        EmailTemplate.Parser emailTemplateParser = new EmailTemplate.Parser();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                assertThat(emailTemplateParser.handle(currentFieldName, parser), is(true));
            }
        }
        EmailTemplate parsedEmailTemplate = emailTemplateParser.parsedTemplate();

        Map<String, Object> model = new HashMap<>();

        HtmlSanitizer htmlSanitizer = mock(HtmlSanitizer.class);
        when(htmlSanitizer.sanitize(htmlBody)).thenReturn(sanitizedHtmlBody);

        Email.Builder emailBuilder = parsedEmailTemplate.render(new MockTextTemplateEngine(), model, htmlSanitizer, new HashMap<>());

        assertThat(emailTemplate.from, equalTo(parsedEmailTemplate.from));
        assertThat(emailTemplate.replyTo, equalTo(parsedEmailTemplate.replyTo));
        assertThat(emailTemplate.priority, equalTo(parsedEmailTemplate.priority));
        assertThat(emailTemplate.to, equalTo(parsedEmailTemplate.to));
        assertThat(emailTemplate.cc, equalTo(parsedEmailTemplate.cc));
        assertThat(emailTemplate.bcc, equalTo(parsedEmailTemplate.bcc));
        assertThat(emailTemplate.subject, equalTo(parsedEmailTemplate.subject));
        assertThat(emailTemplate.textBody, equalTo(parsedEmailTemplate.textBody));
        assertThat(emailTemplate.htmlBody, equalTo(parsedEmailTemplate.htmlBody));

        emailBuilder.id("_id");
        Email email = emailBuilder.build();
        assertThat(email.subject, equalTo(subjectTemplate.getTemplate()));
        assertThat(email.textBody, equalTo(textBodyTemplate.getTemplate()));
        assertThat(email.htmlBody, equalTo(sanitizedHtmlBody));
    }

    public void testParsingMultipleEmailAddresses() throws Exception {
        EmailTemplate template = EmailTemplate.builder()
            .from("sender@example.org")
            .to("to1@example.org, to2@example.org")
            .cc("cc1@example.org, cc2@example.org")
            .bcc("bcc1@example.org, bcc2@example.org")
            .textBody("blah")
            .build();

        Email email = template.render(new MockTextTemplateEngine(), emptyMap(), null, emptyMap()).id("foo").build();

        assertThat(email.to.size(), is(2));
        assertThat(email.to, containsInAnyOrder(new Email.Address("to1@example.org"), new Email.Address("to2@example.org")));
        assertThat(email.cc.size(), is(2));
        assertThat(email.cc, containsInAnyOrder(new Email.Address("cc1@example.org"), new Email.Address("cc2@example.org")));
        assertThat(email.bcc.size(), is(2));
        assertThat(email.bcc, containsInAnyOrder(new Email.Address("bcc1@example.org"), new Email.Address("bcc2@example.org")));
    }

    public void testEmailValidation() {
        assertValidEmail("sender@example.org");
        assertValidEmail("sender+foo@example.org");
        assertValidEmail("Test User <sender@example.org>");
        assertValidEmail("Test User <sender@example.org>, foo@example.org");
        assertValidEmail("a@com");
        assertValidEmail("{{valid due to mustache}}, sender@example.org");
        assertInvalidEmail("lol.com");
        assertInvalidEmail("user");
        // only the whole string is tested if this is a mustache template, not parts of it
        assertValidEmail("{{valid due to mustache}}, lol.com");
    }

    public void testEmailWarning() throws Exception {
        TextTemplate from = randomFrom(new TextTemplate("from@from.com"), null);
        List<TextTemplate> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            addresses.add(new TextTemplate("address" + i + "@test.com"));
        }
        TextTemplate[] possibleList = addresses.toArray(new TextTemplate[addresses.size()]);
        TextTemplate[] replyTo = randomFrom(possibleList, null);
        TextTemplate[] to = randomFrom(possibleList, null);
        TextTemplate[] cc = randomFrom(possibleList, null);
        TextTemplate[] bcc = randomFrom(possibleList, null);
        TextTemplate priority = new TextTemplate(randomFrom(Email.Priority.values()).name());

        TextTemplate subjectTemplate = new TextTemplate("Templated Subject {{foo}}");
        TextTemplate textBodyTemplate = new TextTemplate("Templated Body {{foo}}");

        TextTemplate htmlBodyTemplate = new TextTemplate("Templated Html Body <script>nefarious scripting</script>");
        String htmlBody = "Templated Html Body <script>nefarious scripting</script>";
        String sanitizedHtmlBody = "Templated Html Body";

        EmailTemplate emailTemplate = new EmailTemplate(
            from,
            replyTo,
            priority,
            to,
            cc,
            bcc,
            subjectTemplate,
            textBodyTemplate,
            htmlBodyTemplate
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        emailTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        parser.nextToken();

        EmailTemplate.Parser emailTemplateParser = new EmailTemplate.Parser();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                assertThat(emailTemplateParser.handle(currentFieldName, parser), is(true));
            }
        }
        EmailTemplate parsedEmailTemplate = emailTemplateParser.parsedTemplate();

        Map<String, Object> model = new HashMap<>();

        HtmlSanitizer htmlSanitizer = mock(HtmlSanitizer.class);
        when(htmlSanitizer.sanitize(htmlBody)).thenReturn(sanitizedHtmlBody);
        ArgumentCaptor<String> htmlSanitizeArguments = ArgumentCaptor.forClass(String.class);

        // 4 attachments, zero warning, one warning, two warnings, and one with html that should be stripped
        Map<String, Attachment> attachments = Map.of(
            "one",
            new Attachment.Bytes("one", "one", randomByteArrayOfLength(100), randomAlphaOfLength(5), false, Collections.emptySet()),
            "two",
            new Attachment.Bytes("two", "two", randomByteArrayOfLength(100), randomAlphaOfLength(5), false, Set.of("warning0")),
            "thr",
            new Attachment.Bytes("thr", "thr", randomByteArrayOfLength(100), randomAlphaOfLength(5), false, Set.of("warning1", "warning2")),
            "for",
            new Attachment.Bytes(
                "for",
                "for",
                randomByteArrayOfLength(100),
                randomAlphaOfLength(5),
                false,
                Set.of("<script>warning3</script>")
            )
        );
        Email.Builder emailBuilder = parsedEmailTemplate.render(new MockTextTemplateEngine(), model, htmlSanitizer, attachments);

        emailBuilder.id("_id");
        Email email = emailBuilder.build();
        assertThat(email.subject, equalTo(subjectTemplate.getTemplate()));

        // text
        int bodyStart = email.textBody.indexOf(textBodyTemplate.getTemplate());
        String warnings = email.textBody.substring(0, bodyStart);
        String[] warningLines = warnings.split("\n");
        assertThat(warningLines.length, is(4));
        for (int i = 0; i <= warningLines.length - 1; i++) {
            assertThat(warnings, containsString("warning" + i));
        }

        // html - pull the arguments as it is run through the sanitizer
        verify(htmlSanitizer).sanitize(htmlSanitizeArguments.capture());
        String fullHtmlBody = htmlSanitizeArguments.getValue();
        bodyStart = fullHtmlBody.indexOf(htmlBodyTemplate.getTemplate());
        warnings = fullHtmlBody.substring(0, bodyStart);
        warningLines = warnings.split("<br>");
        assertThat(warningLines.length, is(4));
        for (int i = 0; i <= warningLines.length - 1; i++) {
            assertThat(warnings, containsString("warning" + i));
        }
    }

    private void assertValidEmail(String email) {
        EmailTemplate.Parser.validateEmailAddresses(new TextTemplate(email));
    }

    private void assertInvalidEmail(String email) {
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> EmailTemplate.Parser.validateEmailAddresses(new TextTemplate(email))
        );
        assertThat(e.getMessage(), startsWith("invalid email address"));
    }
}
