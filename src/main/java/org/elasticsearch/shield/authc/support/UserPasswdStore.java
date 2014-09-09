/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

/**
 *
 */
public interface UserPasswdStore {

    boolean verifyPassword(String username, SecuredString password);

    static interface Writable extends UserPasswdStore {

        void store(String username, SecuredString password);

        void remove(String username);

    }

}