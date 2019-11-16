/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RoleDeprecationChecks {

    static DeprecationIssue createPrivilegeCheck(List<RoleDescriptor> roles) {
        final List<String> rolesWithDeprecatedPriv = roles.stream()
            .filter(hasPrivilegeForAnyIndexPredicate("create"))
            .map(RoleDescriptor::getName)
            .sorted() // maintains a consistent ordering so the message is consistent
            .collect(Collectors.toList());
        return new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Some roles use deprecated privilege \"create\"",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html", // TODO: Fix this link
            "Roles which use the deprevated privilege \"create\": "
                + rolesWithDeprecatedPriv.toString()
                + ". Please use the privilege \"index_doc\" instead.");

    }

    private static Predicate<RoleDescriptor> hasPrivilegeForAnyIndexPredicate(String privilege) {
        return role -> Stream.of(role.getIndicesPrivileges())
            .map(RoleDescriptor.IndicesPrivileges::getPrivileges)
            .flatMap(Stream::of)
            .anyMatch(privilege::equals);
    }
}
