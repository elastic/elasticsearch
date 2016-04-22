/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class EmailTemplateTests extends ESTestCase {
    public void testEmailTemplateParserSelfGenerated() throws Exception {
        TextTemplate from = randomFrom(TextTemplate.inline("from@from.com").build(), null);
        List<TextTemplate> addresses = new ArrayList<>();
        for( int i = 0; i < randomIntBetween(1, 5); ++i){
            addresses.add(TextTemplate.inline("address" + i + "@test.com").build());
        }
        TextTemplate[] possibleList = addresses.toArray(new TextTemplate[addresses.size()]);
        TextTemplate[] replyTo = randomFrom(possibleList, null);
        TextTemplate[] to = randomFrom(possibleList, null);
        TextTemplate[] cc = randomFrom(possibleList, null);
        TextTemplate[] bcc = randomFrom(possibleList, null);
        TextTemplate priority = TextTemplate.inline(randomFrom(Email.Priority.values()).name()).build();

        TextTemplate subjectTemplate = TextTemplate.inline("Templated Subject {{foo}}").build();
        String subject = "Templated Subject bar";

        TextTemplate textBodyTemplate = TextTemplate.inline("Templated Body {{foo}}").build();
        String textBody = "Templated Body bar";

        TextTemplate htmlBodyTemplate = TextTemplate.inline("Templated Html Body <script>nefarious scripting</script>").build();
        String htmlBody = "Templated Html Body <script>nefarious scripting</script>";
        String sanitizedHtmlBody = "Templated Html Body";

        EmailTemplate emailTemplate = new EmailTemplate(from, replyTo, priority, to, cc, bcc, subjectTemplate, textBodyTemplate,
                htmlBodyTemplate);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        emailTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        EmailTemplate.Parser emailTemplateParser = new EmailTemplate.Parser();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else  {
                assertThat(emailTemplateParser.handle(currentFieldName, parser), is(true));
            }
        }
        EmailTemplate parsedEmailTemplate = emailTemplateParser.parsedTemplate();

        Map<String, Object> model = new HashMap<>();

        HtmlSanitizer htmlSanitizer = mock(HtmlSanitizer.class);
        when(htmlSanitizer.sanitize(htmlBody)).thenReturn(sanitizedHtmlBody);

        TextTemplateEngine templateEngine = mock(TextTemplateEngine.class);
        when(templateEngine.render(subjectTemplate, model)).thenReturn(subject);
        when(templateEngine.render(textBodyTemplate, model)).thenReturn(textBody);
        when(templateEngine.render(htmlBodyTemplate, model)).thenReturn(htmlBody);
        for (TextTemplate possibleAddress : possibleList) {
            when(templateEngine.render(possibleAddress, model)).thenReturn(possibleAddress.getTemplate());
        }
        if (from != null) {
            when(templateEngine.render(from, model)).thenReturn(from.getTemplate());
        }
        when(templateEngine.render(priority, model)).thenReturn(priority.getTemplate());

        Email.Builder emailBuilder = parsedEmailTemplate.render(templateEngine, model, htmlSanitizer, new HashMap<>());

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
        assertThat(email.subject, equalTo(subject));
        assertThat(email.textBody, equalTo(textBody));
        assertThat(email.htmlBody, equalTo(sanitizedHtmlBody));
    }


}
