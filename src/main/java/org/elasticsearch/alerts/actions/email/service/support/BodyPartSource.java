/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service.support;

import org.elasticsearch.common.xcontent.ToXContent;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;

/**
 *
 */
public abstract class BodyPartSource implements ToXContent {

    protected final String id;
    protected final String name;
    protected final String description;

    public BodyPartSource(String id) {
        this(id, id);
    }

    public BodyPartSource(String id, String name) {
        this(id, name, name);
    }

    public BodyPartSource(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public abstract MimeBodyPart bodyPart() throws MessagingException;

}
