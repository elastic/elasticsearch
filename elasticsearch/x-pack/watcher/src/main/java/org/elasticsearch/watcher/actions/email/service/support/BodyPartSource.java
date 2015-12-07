/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service.support;

import org.elasticsearch.common.xcontent.ToXContent;

import javax.activation.FileTypeMap;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;

/**
 *
 */
public abstract class BodyPartSource implements ToXContent {

    protected static FileTypeMap fileTypeMap = FileTypeMap.getDefaultFileTypeMap();

    protected final String id;
    protected final String name;
    protected final String contentType;

    public BodyPartSource(String id, String contentType) {
        this(id, id, contentType);
    }

    public BodyPartSource(String id, String name, String contentType) {
        this.id = id;
        this.name = name;
        this.contentType = contentType;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String contentType() {
        return contentType;
    }

    public abstract MimeBodyPart bodyPart() throws MessagingException;

}
