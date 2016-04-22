/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.support.secret.SecretService;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/379")
public class ManualPublicSmtpServersTester {

    private static final Terminal terminal = Terminal.DEFAULT;

    public static class Gmail {

        public static void main(String[] args) throws Exception {
            test(Profile.GMAIL, Settings.builder()
                    .put("xpack.notification.email.service.account.gmail.smtp.auth", true)
                    .put("xpack.notification.email.service.account.gmail.smtp.starttls.enable", true)
                    .put("xpack.notification.email.service.account.gmail.smtp.host", "smtp.gmail.com")
                    .put("xpack.notification.email.service.account.gmail.smtp.port", 587)
                    .put("xpack.notification.email.service.account.gmail.smtp.user", terminal.readText("username: "))
                    .put("xpack.notification.email.service.account.gmail.smtp.password", new String(terminal.readSecret("password: ")))
                    .put("xpack.notification.email.service.account.gmail.email_defaults.to", terminal.readText("to: "))
            );
        }
    }

    public static class OutlookDotCom {

        public static void main(String[] args) throws Exception {
            test(Profile.STANDARD, Settings.builder()
                    .put("xpack.notification.email.service.account.outlook.smtp.auth", true)
                    .put("xpack.notification.email.service.account.outlook.smtp.starttls.enable", true)
                    .put("xpack.notification.email.service.account.outlook.smtp.host", "smtp-mail.outlook.com")
                    .put("xpack.notification.email.service.account.outlook.smtp.port", 587)
                    .put("xpack.notification.email.service.account.outlook.smtp.user", "elastic.user@outlook.com")
                    .put("xpack.notification.email.service.account.outlook.smtp.password", "fantastic42")
                    .put("xpack.notification.email.service.account.outlook.email_defaults.to", "elastic.user@outlook.com")
                    .put()
            );
        }
    }

    public static class YahooMail {

        public static void main(String[] args) throws Exception {
            test(Profile.STANDARD, Settings.builder()
                            .put("xpack.notification.email.service.account.yahoo.smtp.starttls.enable", true)
                            .put("xpack.notification.email.service.account.yahoo.smtp.auth", true)
                            .put("xpack.notification.email.service.account.yahoo.smtp.host", "smtp.mail.yahoo.com")
                            .put("xpack.notification.email.service.account.yahoo.smtp.port", 587)
                            .put("xpack.notification.email.service.account.yahoo.smtp.user", "elastic.user@yahoo.com")
                            .put("xpack.notification.email.service.account.yahoo.smtp.password", "fantastic42")
                            // note: from must be set to the same authenticated user account
                            .put("xpack.notification.email.service.account.yahoo.email_defaults.from", "elastic.user@yahoo.com")
                            .put("xpack.notification.email.service.account.yahoo.email_defaults.to", "elastic.user@yahoo.com")
            );
        }
    }

    // Amazon Simple Email Service
    public static class SES {

        public static void main(String[] args) throws Exception {
            test(Profile.STANDARD, Settings.builder()
                            .put("xpack.notification.email.service.account.ses.smtp.auth", true)
                            .put("xpack.notification.email.service.account.ses.smtp.starttls.enable", true)
                            .put("xpack.notification.email.service.account.ses.smtp.starttls.required", true)
                            .put("xpack.notification.email.service.account.ses.smtp.host", "email-smtp.us-east-1.amazonaws.com")
                            .put("xpack.notification.email.service.account.ses.smtp.port", 587)
                            .put("xpack.notification.email.service.account.ses.smtp.user", terminal.readText("user: "))
                            .put("xpack.notification.email.service.account.ses.email_defaults.from", "dummy.user@elasticsearch.com")
                            .put("xpack.notification.email.service.account.ses.email_defaults.to", terminal.readText("to: "))
                            .put("xpack.notification.email.service.account.ses.smtp.password",
                                    new String(terminal.readSecret("password: ")))
            );
        }
    }

    static void test(Profile profile, Settings.Builder builder) throws Exception {
        InternalEmailService service = startEmailService(builder);
        try {

            ToXContent content = new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject()
                            .field("key1", "value1")
                            .field("key2", "value2")
                            .field("key3", "value3")
                            .endObject();
                }
            };

            Email email = Email.builder()
                    .id("_id")
                    .subject("_subject")
                    .textBody("_text_body")
                    .htmlBody("<b>html body</b><p/><p/><img src=\"cid:logo\"/>")
                    .attach(new Attachment.XContent.Yaml("test.yml", content))
                    .inline(new Inline.Stream("logo", "logo.jpg", () -> InternalEmailServiceTests.class.getResourceAsStream("logo.png")))
                    .build();

            EmailService.EmailSent sent = service.send(email, null, profile);

            terminal.println(String.format(Locale.ROOT, "email sent via account [%s]", sent.account()));
        } finally {
            service.stop();
        }
    }

    static InternalEmailService startEmailService(Settings.Builder builder) {
        Settings settings = builder.build();
        InternalEmailService service = new InternalEmailService(settings, SecretService.Insecure.INSTANCE,
                new ClusterSettings(settings, Collections.singleton(InternalEmailService.EMAIL_ACCOUNT_SETTING)));
        service.start();
        return service;
    }
}
