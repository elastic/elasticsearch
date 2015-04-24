/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.email.service.*;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.secret.Secret;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
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
        TemplateEngine engine = mock(TemplateEngine.class);

        EmailTemplate.Builder emailBuilder = EmailTemplate.builder();
        Template subject = null;
        if (randomBoolean()) {
            subject = new Template("_subject");
            emailBuilder.subject(subject);
        }
        Template textBody = null;
        if (randomBoolean()) {
            textBody = new Template("_text_body");
            emailBuilder.textBody(textBody);
        }
        Template htmlBody = null;
        if (randomBoolean()) {
            htmlBody = new Template("_html_body");
            emailBuilder.htmlBody(htmlBody);
        }
        EmailTemplate email = emailBuilder.build();

        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());

        boolean attachPayload = randomBoolean();

        EmailAction action = new EmailAction(email, account, auth, profile, attachPayload);
        ExecutableEmailAction executable = new ExecutableEmailAction(action, logger, service, engine);

        Map<String, Object> data = new HashMap<>();
        Payload payload = new Payload.Simple(data);

        Map<String, Object> metadata = MapBuilder.<String, Object>newMapBuilder().put("_key", "_val").map();

        DateTime now = DateTime.now(UTC);

        Wid wid = new Wid(randomAsciiOfLength(5), randomLong(), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder("watch1")
                .wid(wid)
                .payload(payload)
                .time("watch1", now)
                .metadata(metadata)
                .buildMock();

        Map<String, Object> expectedModel = ImmutableMap.<String, Object>builder()
                .put("ctx", ImmutableMap.<String, Object>builder()
                        .put("watch_id", "watch1")
                        .put("payload", data)
                        .put("metadata", metadata)
                        .put("execution_time", now)
                        .put("trigger", ImmutableMap.<String, Object>builder()
                                .put("triggered_time", now)
                                .put("scheduled_time", now)
                                .build())
                        .build())
                .build();

        if (subject != null) {
            when(engine.render(subject, expectedModel)).thenReturn(subject.getTemplate());
        }
        if (textBody != null) {
            when(engine.render(textBody, expectedModel)).thenReturn(textBody.getTemplate());
        }
        if (htmlBody != null) {
            when(engine.render(htmlBody, expectedModel)).thenReturn(htmlBody.getTemplate());
        }

        EmailAction.Result result = executable.execute("_id", ctx, payload);

        assertThat(result, notNullValue());
        assertThat(result, instanceOf(EmailAction.Result.Success.class));
        assertThat(((EmailAction.Result.Success) result).account(), equalTo(account));
        Email actualEmail = ((EmailAction.Result.Success) result).email();
        assertThat(actualEmail.id(), is(wid.value()));
        assertThat(actualEmail, notNullValue());
        assertThat(actualEmail.subject(), is(subject == null ? null : subject.getTemplate()));
        assertThat(actualEmail.textBody(), is(textBody == null ? null : textBody.getTemplate()));
        assertThat(actualEmail.htmlBody(), is(htmlBody == null ? null : htmlBody.getTemplate()));
        if (attachPayload) {
            assertThat(actualEmail.attachments(), hasKey("data"));
        }
    }

    @Test @Repeat(iterations = 20)
    public void testParser() throws Exception {
        TemplateEngine engine = mock(TemplateEngine.class);
        EmailService emailService = mock(EmailService.class);
        Profile profile = randomFrom(Profile.values());
        Email.Priority priority = randomFrom(Email.Priority.values());
        Email.Address[] to = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "to@domain" : "to1@domain,to2@domain").toArray();
        Email.Address[] cc = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "cc@domain" : "cc1@domain,cc2@domain").toArray();
        Email.Address[] bcc = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "bcc@domain" : "bcc1@domain,bcc2@domain").toArray();
        Email.Address[] replyTo = rarely() ? null : Email.AddressList.parse(randomBoolean() ? "reply@domain" : "reply1@domain,reply2@domain").toArray();
        Template subject = randomBoolean() ? new Template("_subject") : null;
        Template textBody = randomBoolean() ? new Template("_text_body") : null;
        Template htmlBody = randomBoolean() ? new Template("_text_html") : null;
        boolean attachData = randomBoolean();
        XContentBuilder builder = jsonBuilder().startObject()
                .field("account", "_account")
                .field("profile", profile.name())
                .field("user", "_user")
                .field("password", "_passwd")
                .field("attach_data", attachData)
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
                builder.field("subject", subject.getTemplate());
            } else {
                builder.field("subject", subject);
            }
        }
        if (textBody != null) {
            if (randomBoolean()) {
                builder.field("text_body", textBody.getTemplate());
            } else {
                builder.field("text_body", textBody);
            }
        }
        if (htmlBody != null) {
            if (randomBoolean()) {
                builder.field("html_body", htmlBody.getTemplate());
            } else {
                builder.field("html_body", htmlBody);
            }
        }
        BytesReference bytes = builder.bytes();
        logger.info("email action json [{}]", bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        ExecutableEmailAction executable = new EmailActionFactory(ImmutableSettings.EMPTY, emailService, engine)
                .parseExecutable(randomAsciiOfLength(8), randomAsciiOfLength(3), parser);

        assertThat(executable, notNullValue());
        assertThat(executable.action().getAccount(), is("_account"));
        assertThat(executable.action().getAttachData(), is(attachData));
        assertThat(executable.action().getAuth(), notNullValue());
        assertThat(executable.action().getAuth().user(), is("_user"));
        assertThat(executable.action().getAuth().password(), is(new Secret("_passwd".toCharArray())));
        assertThat(executable.action().getEmail().priority(), is(new Template(priority.name())));
        if (to != null) {
            assertThat(executable.action().getEmail().to(), arrayContainingInAnyOrder(addressesToTemplates(to)));
        } else {
            assertThat(executable.action().getEmail().to(), nullValue());
        }
        if (cc != null) {
            assertThat(executable.action().getEmail().cc(), arrayContainingInAnyOrder(addressesToTemplates(cc)));
        } else {
            assertThat(executable.action().getEmail().cc(), nullValue());
        }
        if (bcc != null) {
            assertThat(executable.action().getEmail().bcc(), arrayContainingInAnyOrder(addressesToTemplates(bcc)));
        } else {
            assertThat(executable.action().getEmail().bcc(), nullValue());
        }
        if (replyTo != null) {
            assertThat(executable.action().getEmail().replyTo(), arrayContainingInAnyOrder(addressesToTemplates(replyTo)));
        } else {
            assertThat(executable.action().getEmail().replyTo(), nullValue());
        }
    }

    private static Template[] addressesToTemplates(Email.Address[] addresses) {
        Template[] templates = new Template[addresses.length];
        for (int i = 0; i < templates.length; i++) {
            templates[i] = new Template(addresses[i].toString());
        }
        return templates;
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SelfGenerated() throws Exception {
        EmailService service = mock(EmailService.class);
        TemplateEngine engine = mock(TemplateEngine.class);
        EmailTemplate.Builder emailTemplate = EmailTemplate.builder();
        if (randomBoolean()) {
            emailTemplate.from("from@domain");
        }
        if (randomBoolean()) {
            emailTemplate.to(randomBoolean() ? "to@domain" : "to1@domain,to2@domain");
        }
        if (randomBoolean()) {
            emailTemplate.cc(randomBoolean() ? "cc@domain" : "cc1@domain,cc2@domain");
        }
        if (randomBoolean()) {
            emailTemplate.bcc(randomBoolean() ? "bcc@domain" : "bcc1@domain,bcc2@domain");
        }
        if (randomBoolean()) {
            emailTemplate.replyTo(randomBoolean() ? "reply@domain" : "reply1@domain,reply2@domain");
        }
        EmailTemplate email = emailTemplate.build();
        Authentication auth = randomBoolean() ? null : new Authentication("_user", new Secret("_passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());
        String account = randomAsciiOfLength(6);
        boolean attachPayload = randomBoolean();

        EmailAction action = new EmailAction(email, account, auth, profile, attachPayload);
        ExecutableEmailAction executable = new ExecutableEmailAction(action, logger, service, engine);

        boolean hideSecrets = randomBoolean();
        ToXContent.Params params = WatcherParams.builder().hideSecrets(hideSecrets).build();

        XContentBuilder builder = jsonBuilder();
        executable.toXContent(builder, params);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        ExecutableEmailAction parsed = new EmailActionFactory(ImmutableSettings.EMPTY, service, engine)
                .parseExecutable(randomAsciiOfLength(4), randomAsciiOfLength(10), parser);

        if (!hideSecrets) {
            assertThat(parsed, equalTo(executable));
        } else {
            assertThat(parsed.action().getAccount(), is(executable.action().getAccount()));
            assertThat(parsed.action().getEmail(), is(executable.action().getEmail()));
            assertThat(parsed.action().getAttachData(), is(executable.action().getAttachData()));
            if (auth != null) {
                assertThat(parsed.action().getAuth().user(), is(executable.action().getAuth().user()));
                assertThat(parsed.action().getAuth().password(), nullValue());
                assertThat(executable.action().getAuth().password(), notNullValue());
            }
        }

    }

    @Test(expected = EmailActionException.class) @Repeat(iterations = 100)
    public void testParser_Invalid() throws Exception {
        EmailService emailService = mock(EmailService.class);
        TemplateEngine engine = mock(TemplateEngine.class);
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value");
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        new EmailActionFactory(ImmutableSettings.EMPTY, emailService, engine)
                .parseExecutable(randomAsciiOfLength(3), randomAsciiOfLength(7), parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_Result() throws Exception {
        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now(UTC));
        String actionId = randomAsciiOfLength(5);

        boolean success = randomBoolean();
        boolean simulated = randomBoolean();
        Email email = Email.builder().id("_id")
                .from(new Email.Address("from@domain"))
                .to(Email.AddressList.parse("to@domain"))
                .sentDate(new DateTime(UTC))
                .subject("_subject")
                .textBody("_text_body")
                .build();

        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", success);
        if (simulated) {
            builder.field("simulated_email", email);
        }
        else if (success) {
            builder.field("email", email);
            builder.field("account", "_account");
        } else {
            builder.field("reason", "_reason");
        }
        builder.endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        EmailService service = mock(EmailService.class);
        TemplateEngine engine = mock(TemplateEngine.class);
        EmailAction.Result result = new EmailActionFactory(ImmutableSettings.EMPTY, service, engine)
                .parseResult(wid, actionId, parser);

        if (simulated) {
            assertThat(result, instanceOf(EmailAction.Result.Simulated.class));
            assertThat(((EmailAction.Result.Simulated) result).email(), equalTo(email));
        } else {
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
    }

    @Test
    public void testParser_Result_Simulated_SelfGenerated() throws Exception {
        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now(UTC));
        String actionId = randomAsciiOfLength(5);

        Email email = Email.builder().id("_id")
                .from(new Email.Address("from@domain"))
                .to(Email.AddressList.parse("to@domain"))
                .sentDate(new DateTime(UTC))
                .subject("_subject")
                .textBody("_text_body")
                .build();

        EmailAction.Result.Simulated simulatedResult = new EmailAction.Result.Simulated(email);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        simulatedResult.toXContent(builder, ToXContent.EMPTY_PARAMS);

        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        EmailAction.Result result = new EmailActionFactory(ImmutableSettings.EMPTY, mock(EmailService.class), mock(TemplateEngine.class))
                .parseResult(wid, actionId, parser);

        assertThat(result, instanceOf(EmailAction.Result.Simulated.class));
        assertThat(((EmailAction.Result.Simulated) result).email(), equalTo(email));
    }

    @Test(expected = EmailActionException.class)
    public void testParser_Result_Invalid() throws Exception {
        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now(UTC));
        String actionId = randomAsciiOfLength(5);

        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value")
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        new EmailActionFactory(ImmutableSettings.EMPTY, mock(EmailService.class), mock(TemplateEngine.class))
                .parseResult(wid, actionId, parser);
    }
}
