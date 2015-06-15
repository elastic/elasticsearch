/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.junit.Test;

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
public class EmailTemplateTests extends ElasticsearchTestCase {

    @Test
    public void testEmailTemplate_Parser_SelfGenerated() throws Exception {
        Template from = randomFrom(Template.inline("from@from.com").build(), null);
        List<Template> addresses = new ArrayList<>();
        for( int i = 0; i < randomIntBetween(1, 5); ++i){
            addresses.add(Template.inline("address" + i + "@test.com").build());
        }
        Template[] possibleList = addresses.toArray(new Template[addresses.size()]);
        Template[] replyTo = randomFrom(possibleList, null);
        Template[] to = randomFrom(possibleList, null);
        Template[] cc = randomFrom(possibleList, null);
        Template[] bcc = randomFrom(possibleList, null);
        Template priority = Template.inline(randomFrom(Email.Priority.values()).name()).build();
        boolean sanitizeHtml = randomBoolean();

        Template templatedSubject = Template.inline("Templated Subject {{foo}}").build();
        String renderedTemplatedSubject = "Templated Subject bar";

        Template templatedBody = Template.inline("Templated Body {{foo}}").build();
        String renderedTemplatedBody = "Templated Body bar";

        Template templatedHtmlBodyGood = Template.inline("Templated Html Body <hr />").build();
        String renderedTemplatedHtmlBodyGood = "Templated Html Body <hr /> bar";

        Template templatedHtmlBodyBad = Template.inline("Templated Html Body <script>nefarious scripting</script>").build();
        String renderedTemplatedHtmlBodyBad = "Templated Html Body<script>nefarious scripting</script>";
        String renderedSanitizedHtmlBodyBad = "Templated Html Body";

        Template htmlBody = randomFrom(templatedHtmlBodyGood, templatedHtmlBodyBad);

        EmailTemplate emailTemplate = new EmailTemplate(from, replyTo, priority, to, cc, bcc,
                templatedSubject, templatedBody, htmlBody, sanitizeHtml);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        emailTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        EmailTemplate.Parser emailTemplateParser = new EmailTemplate.Parser(sanitizeHtml);

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
        TemplateEngine templateEngine = mock(TemplateEngine.class);
        when(templateEngine.render(templatedSubject, model)).thenReturn(renderedTemplatedSubject);
        when(templateEngine.render(templatedBody, model)).thenReturn(renderedTemplatedBody);
        when(templateEngine.render(templatedHtmlBodyGood, model)).thenReturn(renderedTemplatedHtmlBodyGood);
        when(templateEngine.render(templatedHtmlBodyBad, model)).thenReturn(renderedTemplatedHtmlBodyBad);
        for (Template possibleAddress : possibleList) {
            when(templateEngine.render(possibleAddress, model)).thenReturn(possibleAddress.getTemplate());
        }
        if (from != null) {
            when(templateEngine.render(from, model)).thenReturn(from.getTemplate());
        }
        when(templateEngine.render(priority, model)).thenReturn(priority.getTemplate());

        Email.Builder emailBuilder = parsedEmailTemplate.render(templateEngine, model, new HashMap<String, Attachment>());

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
        assertThat(email.subject, equalTo(renderedTemplatedSubject));
        assertThat(email.textBody, equalTo(renderedTemplatedBody));
        if (htmlBody.equals(templatedHtmlBodyBad)) {
            if (sanitizeHtml) {
                assertThat(email.htmlBody, equalTo(renderedSanitizedHtmlBodyBad));
            } else {
                assertThat(email.htmlBody, equalTo(renderedTemplatedHtmlBodyBad));
            }
        } else {
            assertThat(email.htmlBody, equalTo(renderedTemplatedHtmlBodyGood));
        }
    }


}
