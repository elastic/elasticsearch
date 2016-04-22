/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.elasticsearch.common.component.LifecycleComponent;

import javax.mail.MessagingException;

/**
 *
 */
public interface EmailService extends LifecycleComponent<EmailService>{

    EmailSent send(Email email, Authentication auth, Profile profile) throws MessagingException;

    EmailSent send(Email email, Authentication auth, Profile profile, String accountName) throws MessagingException;

    class EmailSent {

        private final String account;
        private final Email email;

        public EmailSent(String account, Email email) {
            this.account = account;
            this.email = email;
        }

        public String account() {
            return account;
        }

        public Email email() {
            return email;
        }
    }

}
