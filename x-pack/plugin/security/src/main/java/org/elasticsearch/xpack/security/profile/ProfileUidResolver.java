/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;

/**
 * Resolves the profile uid of the authenticated user for document level security templates that refer to
 * {@code _user.profile_uid}, answering {@code null} when the subject has none. See {@link ProfileService#resolve}.
 */
@FunctionalInterface
public interface ProfileUidResolver {
    ProfileUidResolver NONE = (authentication, listener) -> listener.onResponse(null);

    void resolve(Authentication authentication, ActionListener<String> listener);
}
