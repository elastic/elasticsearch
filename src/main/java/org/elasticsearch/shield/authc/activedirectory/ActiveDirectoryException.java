/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import org.elasticsearch.shield.ShieldException;

/**
 * ActiveDirectoryExceptions typically wrap {@link com.unboundid.ldap.sdk.LDAPException}, and have an additional
 * parameter of DN attached to each message.
 */
public class ActiveDirectoryException extends ShieldException {

    public ActiveDirectoryException(String msg){
        super(msg);
    }

    public ActiveDirectoryException(String msg, Throwable cause){
        super(msg, cause);
    }

    public ActiveDirectoryException(String msg, String dn) {
        this(msg, dn, null);
    }

    public ActiveDirectoryException(String msg, String dn, Throwable cause) {
        super( msg + "; DN=[" + dn + "]", cause);
    }
}
