/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.support;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.xcontent.ToXContentObject;

import java.security.AccessController;
import java.security.PrivilegedAction;

import javax.activation.FileTypeMap;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;

public abstract class BodyPartSource implements ToXContentObject {

    protected static FileTypeMap fileTypeMap;
    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        fileTypeMap = AccessController.doPrivileged((PrivilegedAction<FileTypeMap>) () -> FileTypeMap.getDefaultFileTypeMap());
    }

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

    // exists only to allow ensuring class is initialized
    public static void init() {}

}
