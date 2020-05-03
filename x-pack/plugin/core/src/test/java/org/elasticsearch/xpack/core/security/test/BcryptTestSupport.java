/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.test;

import org.elasticsearch.xpack.core.security.authc.support.BCrypt;

import java.util.function.Consumer;

public class BcryptTestSupport {

    public static void runWithValidRevisions(String baseHash, Consumer<char[]> action) {
        for (char revision : BCrypt.REVISIONS) {
            char[] hash = toCharArrayWithNewRevision(baseHash, revision);
            action.accept(hash);
        }
    }

    public static void runWithInvalidRevisions(String baseHash, Consumer<char[]> action) {
        for (char revision = 'a'; revision <= 'z'; revision++) {
            if (BCrypt.REVISIONS.contains(revision) == false) {
                char[] hash = toCharArrayWithNewRevision(baseHash, revision);
                action.accept(hash);
            }
        }
    }

    public static char[] toCharArrayWithNewRevision(String hash, char newRevision) {
        final int REVISION_INDEX = 2;
        char[] hashBuilder = hash.toCharArray();
        if (hash.length() >= (REVISION_INDEX + 1)) {
            hashBuilder[REVISION_INDEX] = newRevision;
        }
        return hashBuilder;
    }
}
