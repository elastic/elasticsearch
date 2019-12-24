/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.mail.BodyPart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.util.HashSet;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ProfileTests extends ESTestCase {

    public void testThatInlineAttachmentsAreCreated() throws Exception {
        String path = "/org/elasticsearch/xpack/watcher/actions/email/service/logo.png";
        Attachment attachment = new Attachment.Stream("inline.png", "inline.png", true,
                () -> EmailServiceTests.class.getResourceAsStream(path));

        Email email = Email.builder()
                .id("foo")
                .from("foo@example.org")
                .to("bar@example.org")
                .subject(randomAlphaOfLength(10))
                .attach(attachment)
                .build();

        Settings settings = Settings.builder()
                .put("xpack.notification.email.default_account", "foo")
                .put("xpack.notification.email.account.foo.smtp.host", "_host")
                .build();

        EmailService service = new EmailService(settings, null, mock(SSLService.class),
                new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings())));
        Session session = service.getAccount("foo").getConfig().createSession();
        MimeMessage mimeMessage = Profile.STANDARD.toMimeMessage(email, session);

        Object content = ((MimeMultipart) mimeMessage.getContent()).getBodyPart(0).getContent();
        assertThat(content, instanceOf(MimeMultipart.class));
        MimeMultipart multipart = (MimeMultipart) content;

        assertThat(multipart.getCount(), is(2));
        boolean foundInlineAttachment = false;
        BodyPart bodyPart = null;
        for (int i = 0; i < multipart.getCount(); i++) {
            bodyPart = multipart.getBodyPart(i);
            if (Part.INLINE.equalsIgnoreCase(bodyPart.getDisposition())) {
                foundInlineAttachment = true;
                break;
            }
        }

        assertThat("Expected to find an inline attachment in mime message, but didnt", foundInlineAttachment, is(true));
    }
}
