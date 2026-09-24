/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountAuthor;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Fills in the profile uids of the creators and editors of user-managed accounts, for the get and query APIs when a
 * caller asks for them with {@code with_profile_uid}. Built-in accounts have no authors and pass through untouched.
 */
final class ServiceAccountAuthorProfileUids {

    private ServiceAccountAuthorProfileUids() {}

    /**
     * Answers with the given accounts, in order, each user-managed one carrying the profile uids of its authors. The
     * order is relied on: the query action pairs the answer with its items' sort values by position.
     * <p>
     * Authors are sent as one list, creator then editor per account, and uids come back one per author in that order,
     * as for API key owners. Reading them back walks the accounts the same way, so the two loops must agree on which
     * authors are present. A {@code null} answer means there is no profile index, and the accounts are returned as
     * given.
     */
    static void resolve(ProfileService profileService, List<ServiceAccountInfo> infos, ActionListener<List<ServiceAccountInfo>> listener) {
        final List<ServiceAccountAuthor> authors = new ArrayList<>();
        for (ServiceAccountInfo info : infos) {
            if (info instanceof ServiceAccountInfo.UserManaged userManaged) {
                if (userManaged.creator() != null) {
                    authors.add(userManaged.creator());
                }
                if (userManaged.editor() != null) {
                    authors.add(userManaged.editor());
                }
            }
        }
        if (authors.isEmpty()) {
            listener.onResponse(infos);
            return;
        }
        profileService.resolveProfileUidsForServiceAccountAuthors(authors, listener.map(profileUids -> {
            if (profileUids == null) {
                return infos;
            }
            assert profileUids.size() == authors.size() : "expected one profile uid per author";
            final Iterator<String> uids = profileUids.iterator();
            final List<ServiceAccountInfo> resolved = new ArrayList<>(infos.size());
            for (ServiceAccountInfo info : infos) {
                if (info instanceof ServiceAccountInfo.UserManaged userManaged) {
                    final String creatorProfileUid = userManaged.creator() == null ? null : uids.next();
                    final String editorProfileUid = userManaged.editor() == null ? null : uids.next();
                    resolved.add(userManaged.withProfileUids(creatorProfileUid, editorProfileUid));
                } else {
                    resolved.add(info);
                }
            }
            return resolved;
        }));
    }
}
