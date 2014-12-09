/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.shield.ShieldException;

/**
 * LdapExceptions typically wrap jndi Naming exceptions, and have an additional
 * parameter of DN attached to each message.
 */
public class LdapException extends ShieldException {

    public LdapException(String msg){
        super(msg);
    }

    public LdapException(String msg, Throwable cause){
        super(msg, cause);
    }

    public LdapException(String msg, String dn) {
        this(msg, dn, null);
    }

    public LdapException(String msg, String dn, Throwable cause) {
        super( msg + "; LDAP DN=[" + dn + "]", cause);
    }
}
