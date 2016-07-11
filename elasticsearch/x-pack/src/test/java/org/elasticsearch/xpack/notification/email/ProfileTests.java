/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import javax.mail.BodyPart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ProfileTests extends ESTestCase {

    public void testThatInlineAttachmentsAreCreated() throws Exception {
        String path = "/org/elasticsearch/xpack/watcher/actions/email/service/logo.png";
        Attachment attachment = new Attachment.Stream("inline.png", "inline.png", true,
                () -> InternalEmailServiceTests.class.getResourceAsStream(path));

        Email email = Email.builder()
                .id("foo")
                .from("foo@example.org")
                .to("bar@example.org")
                .subject(randomAsciiOfLength(10))
                .attach(attachment)
                .build();

        Settings settings = Settings.builder()
                .put("default_account", "foo")
                .put("account.foo.smtp.host", "_host")
                .build();

        Accounts accounts = new Accounts(settings, null, logger);
        Session session = accounts.account("foo").getConfig().createSession();
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