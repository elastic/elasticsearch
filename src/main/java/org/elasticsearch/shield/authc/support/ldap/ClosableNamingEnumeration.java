/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.shield.authc.ldap.LdapException;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import java.io.Closeable;

/**
 * ClosableNamingEnumeration wraps a NamingEnumeration so it can be used in a try with resources block and auto-closed.
 */
public class ClosableNamingEnumeration<T> implements Closeable, NamingEnumeration<T> {

    private final NamingEnumeration<T> namingEnumeration;

    public ClosableNamingEnumeration(NamingEnumeration<T> namingEnumeration) {
        this.namingEnumeration = namingEnumeration;
    }

    @Override
    public T next() throws NamingException {
        return namingEnumeration.next();
    }

    @Override
    public boolean hasMore() throws NamingException {
        return namingEnumeration.hasMore();
    }

    @Override
    public void close() {
        try {
            namingEnumeration.close();
        } catch (NamingException e) {
            throw new LdapException("error occurred trying to close a naming enumeration", e);
        }
    }

    @Override
    public boolean hasMoreElements() {
        return namingEnumeration.hasMoreElements();
    }

    @Override
    public T nextElement() {
        return namingEnumeration.nextElement();
    }
}
