/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.actions.ActionSettingsException;
import org.elasticsearch.alerts.actions.email.service.*;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.support.template.ScriptTemplate;
import org.elasticsearch.alerts.support.template.Template;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class EmailActionTests extends ElasticsearchTestCase {

    @Test @Repeat(iterations = 20)
    public void testExecute() throws Exception {
        final String account = "account1";
        EmailService service = new EmailService() {
            @Override
            public EmailSent send(Email email, Authentication auth, Profile profile) {
                return new EmailSent(account, email);
            }

            @Override
            public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
                return new EmailSent(account, email);
            }
        };
        Email email = Email.builder().id("prototype").build();
        Authentication auth = new Authentication("user", "passwd");
        Profile profile = randomFrom(Profile.values());

        Template subject = mock(Template.class);
        Template textBody = mock(Template.class);
        Template htmlBody = randomBoolean() ? null : mock(Template.class);
        boolean attachPayload = randomBoolean();
        EmailAction action = new EmailAction(logger, service, email, auth, profile, account, subject, textBody, htmlBody, attachPayload);

        final Map<String, Object> data = new HashMap<>();
        Payload payload = new Payload() {
            @Override
            public Map<String, Object> data() {
                return data;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.map(data);
            }
        };

        DateTime now = new DateTime(DateTimeZone.UTC);

        String ctxId = randomAsciiOfLength(5);
        ExecutionContext ctx = mock(ExecutionContext.class);
        when(ctx.id()).thenReturn(ctxId);
        Alert alert = mock(Alert.class);
        when(alert.name()).thenReturn("alert1");
        when(ctx.alert()).thenReturn(alert);
        when(ctx.fireTime()).thenReturn(now);
        when(ctx.scheduledTime()).thenReturn(now);

        Map<String, Object> expectedModel = ImmutableMap.<String, Object>builder()
                .put("ctx", ImmutableMap.<String, Object>builder()
                    .put("alert_name", "alert1")
                    .put("payload", data)
                    .put("fire_time", now)
                    .put("scheduled_fire_time", now).build())
                .build();

        when(subject.render(expectedModel)).thenReturn("_subject");
        when(textBody.render(expectedModel)).thenReturn("_text_body");
        if (htmlBody != null) {
            when (htmlBody.render(expectedModel)).thenReturn("_html_body");
        }

        EmailAction.Result result = action.execute(ctx, payload);

        assertThat(result, notNullValue());
        assertThat(result, instanceOf(EmailAction.Result.Success.class));
        assertThat(((EmailAction.Result.Success) result).account(), equalTo(account));
        Email actualEmail = ((EmailAction.Result.Success) result).email();
        assertThat(actualEmail.id(), is(ctxId));
        assertThat(actualEmail, notNullValue());
        assertThat(actualEmail.subject(), is("_subject"));
        assertThat(actualEmail.textBody(), is("_text_body"));
        if (htmlBody != null) {
            assertThat(actualEmail.htmlBody(), is("_html_body"));
        }
        if (attachPayload) {
            assertThat(actualEmail.attachments(), hasKey("payload"));
        }
    }

    @Test @Repeat(iterations = 20)
    public void testParser() throws Exception {
        ScriptServiceProxy scriptService = mock(ScriptServiceProxy.class);
        EmailService emailService = mock(EmailService.class);
        Profile profile = randomFrom(Profile.values());
        Email.Priority priority = randomFrom(Email.Priority.values());
        Email.Address[] to = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "to@domain" : "to1@domain,to2@domain").toArray();
        Email.Address[] cc = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "cc@domain" : "cc1@domain,cc2@domain").toArray();
        Email.Address[] bcc = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "bcc@domain" : "bcc1@domain,bcc2@domain").toArray();
        Email.Address[] replyTo = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "reply@domain" : "reply1@domain,reply2@domain").toArray();
        ScriptTemplate subject = randomBoolean() ? new ScriptTemplate(scriptService, "_subject") : null;
        ScriptTemplate textBody = randomBoolean() ? new ScriptTemplate(scriptService, "_text_body") : null;
        ScriptTemplate htmlBody = randomBoolean() ? new ScriptTemplate(scriptService, "_text_html") : null;
        boolean attachPayload = randomBoolean();
        XContentBuilder builder = jsonBuilder().startObject()
                .field("account", "_account")
                .field("profile", profile.name())
                .field("user", "_user")
                .field("password", "_passwd")
                .field("attach_payload", attachPayload)
                .field("from", "from@domain")
                .field("priority", priority.name());
        if (to != null) {
            if (to.length == 1) {
                builder.field("to", to[0]);
            } else {
                builder.array("to", (Object[]) to);
            }
        }
        if (cc != null) {
            if (cc.length == 1) {
                builder.field("cc", cc[0]);
            } else {
                builder.array("cc", (Object[]) cc);
            }
        }
        if (bcc != null) {
            if (bcc.length == 1) {
                builder.field("bcc", bcc[0]);
            } else {
                builder.array("bcc", (Object[]) bcc);
            }
        }
        if (replyTo != null) {
            if (replyTo.length == 1) {
                builder.field("reply_to", replyTo[0]);
            } else {
                builder.array("reply_to", (Object[]) replyTo);
            }
        }
        if (subject != null) {
            if (randomBoolean()) {
                builder.field("subject", subject.script().script());
            } else {
                builder.field("subject", subject);
            }
        }
        if (textBody != null) {
            if (randomBoolean()) {
                builder.field("text_body", textBody.script().script());
            } else {
                builder.field("text_body", textBody);
            }
        }
        if (htmlBody != null) {
            if (randomBoolean()) {
                builder.field("html_body", htmlBody.script().script());
            } else {
                builder.field("html_body", htmlBody);
            }
        }
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        EmailAction action = new EmailAction.Parser(ImmutableSettings.EMPTY, emailService,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService)).parse(parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is("_account"));
        assertThat(action.attachPayload, is(attachPayload));
        assertThat(action.auth, notNullValue());
        assertThat(action.auth.user(), is("_user"));
        assertThat(action.auth.password(), is("_passwd"));
        assertThat(action.subject, is((Template) subject));
        assertThat(action.textBody, is((Template) textBody));
        assertThat(action.htmlBody, is((Template) htmlBody));
        assertThat(action.emailPrototype.priority(), is(priority));
        if (to != null) {
            assertThat(action.emailPrototype.to().toArray(), arrayContainingInAnyOrder(to));
        } else {
            assertThat(action.emailPrototype.to(), nullValue());
        }
        if (cc != null) {
            assertThat(action.emailPrototype.cc().toArray(), arrayContainingInAnyOrder(cc));
        } else {
            assertThat(action.emailPrototype.cc(), nullValue());
        }
        if (bcc != null) {
            assertThat(action.emailPrototype.bcc().toArray(), arrayContainingInAnyOrder(bcc));
        } else {
            assertThat(action.emailPrototype.bcc(), nullValue());
        }
        if (replyTo != null) {
            assertThat(action.emailPrototype.replyTo().toArray(), arrayContainingInAnyOrder(replyTo));
        } else {
            assertThat(action.emailPrototype.replyTo(), nullValue());
        }
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SelfGenerated() throws Exception {
        EmailService service = mock(EmailService.class);
        Email.Builder emailPrototypeBuilder = Email.builder().id("prototype");
        if (randomBoolean()) {
            emailPrototypeBuilder.from(new Email.Address("from@domain"));
        }
        if (randomBoolean()) {
            emailPrototypeBuilder.to(Email.AddressList.parse(randomBoolean() ? "to@domain" : "to1@domain,to2@damain"));
        }
        if (randomBoolean()) {
            emailPrototypeBuilder.cc(Email.AddressList.parse(randomBoolean() ? "cc@domain" : "cc1@domain,cc2@damain"));
        }
        if (randomBoolean()) {
            emailPrototypeBuilder.bcc(Email.AddressList.parse(randomBoolean() ? "bcc@domain" : "bcc1@domain,bcc2@damain"));
        }
        if (randomBoolean()) {
            emailPrototypeBuilder.replyTo(Email.AddressList.parse(randomBoolean() ? "reply@domain" : "reply1@domain,reply2@damain"));
        }
        Email email = emailPrototypeBuilder.build();
        Authentication auth = randomBoolean() ? null : new Authentication("_user", "_passwd");
        Profile profile = randomFrom(Profile.values());
        String account = randomAsciiOfLength(6);
        Template subject = new TemplateMock("_subject");
        Template textBody = new TemplateMock("_text_body");
        Template htmlBody = randomBoolean() ? null : new TemplateMock("_html_body");
        boolean attachPayload = randomBoolean();

        EmailAction action = new EmailAction(logger, service, email, auth, profile, account, subject, textBody, htmlBody, attachPayload);

        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        EmailAction parsed = new EmailAction.Parser(ImmutableSettings.EMPTY, service, new TemplateMock.Parser()).parse(parser);
        assertThat(parsed, equalTo(action));

    }

    @Test(expected = ActionSettingsException.class) @Repeat(iterations = 100)
    public void testParser_Invalid() throws Exception {
        ScriptServiceProxy scriptService = mock(ScriptServiceProxy.class);
        EmailService emailService = mock(EmailService.class);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value");
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        new EmailAction.Parser(ImmutableSettings.EMPTY, emailService,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService)).parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_Result() throws Exception {
        boolean success = randomBoolean();
        Email email = Email.builder().id("_id")
                .from(new Email.Address("from@domain"))
                .to(Email.AddressList.parse("to@domain"))
                .sentDate(new DateTime())
                .subject("_subject")
                .textBody("_text_body")
                .build();
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", success);
        if (success) {
            builder.field("email", email);
            builder.field("account", "_account");
        } else {
            builder.field("reason", "_reason");
        }
        builder.endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        EmailAction.Result result = new EmailAction.Parser(ImmutableSettings.EMPTY, mock(EmailService.class), new TemplateMock.Parser())
                .parseResult(parser);
        assertThat(result.success(), is(success));
        if (success) {
            assertThat(result, instanceOf(EmailAction.Result.Success.class));
            assertThat(((EmailAction.Result.Success) result).email(), equalTo(email));
            assertThat(((EmailAction.Result.Success) result).account(), is("_account"));
        } else {
            assertThat(result, instanceOf(EmailAction.Result.Failure.class));
            assertThat(((EmailAction.Result.Failure) result).reason(), is("_reason"));
        }
    }

    @Test(expected = EmailException.class)
    public void testParser_Result_Invalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value")
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        new EmailAction.Parser(ImmutableSettings.EMPTY, mock(EmailService.class), new TemplateMock.Parser())
                .parseResult(parser);
    }

    static class TemplateMock implements Template {

        private final String name;

        public TemplateMock(String name) {
            this.name = name;
        }

        @Override
        public String render(Map<String, Object> model) {
            return "";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TemplateMock that = (TemplateMock) o;

            if (!name.equals(that.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        public static class Parser implements Template.Parser<TemplateMock> {

            @Override
            public TemplateMock parse(XContentParser parser) throws IOException, ParseException {
                return new TemplateMock(parser.text());
            }
        }
    }
}
