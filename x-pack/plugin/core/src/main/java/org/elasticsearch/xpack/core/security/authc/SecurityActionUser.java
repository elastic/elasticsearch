/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.support.user.ActionUser;

/**
 * An implementation of {@link org.elasticsearch.action.support.user.ActionUser} that is backed by Elasticsearch (X-Pack) security
 */
public record SecurityActionUser(Subject subject) implements ActionUser {

    @Override
    public String identity() {
        if (subject.getType() == Subject.Type.SERVICE_ACCOUNT) {
            return subject.getUser().principal();
        }
        final String username = subject.getUser().principal() + "@" + subject.getRealm().getType() + "." + subject.getRealm().getName();
        if (subject.getType() == Subject.Type.USER) {
            return username;
        } else if (subject.getType() == Subject.Type.API_KEY) {
            final String apiKeyId = (String) subject.getMetadata().get(AuthenticationField.API_KEY_ID_KEY);
            return apiKeyId + ":" + username;
        } else {
            assert false : "Unrecognized subject type";
            return subject.getType() + "::" + username;
        }
    }

}
