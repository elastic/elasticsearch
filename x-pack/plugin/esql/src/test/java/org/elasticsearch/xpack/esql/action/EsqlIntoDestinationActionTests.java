/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.List;

/**
 * The action is authorized by name, so the privileges that decide who may write to the destination of an {@code INTO} are the ones
 * whose patterns match it. A change to the privileges, or to the name, that breaks this would let a caller write who is refused, or
 * refuse one who may write.
 */
public class EsqlIntoDestinationActionTests extends ESTestCase {
    /** What the filter authorizes for each document that a bulk creates, so that a custom privilege grants the same to both. */
    public void testTheActionIsTheCreationOfADocumentByABulk() {
        assertEquals("indices:data/write/index:op_type/create", EsqlIntoDestinationAction.NAME);
    }

    public void testThePrivilegesThatAllowIndexingADocumentGrantTheAction() {
        for (String privilege : List.of("create_doc", "create", "index", "write", "all")) {
            assertTrue(privilege, IndexPrivilege.get(privilege).predicate().test(EsqlIntoDestinationAction.NAME));
        }
    }

    /** What the filter authorizes for each document that a bulk indexes, which overwrites, so it is not a creation. */
    public void testTheReplacingActionIsTheIndexingOfADocumentByABulk() {
        assertEquals("indices:data/write/index:op_type/index", EsqlIntoReplaceDestinationAction.NAME);
    }

    /** {@code create_doc} only allows creating a document, so a caller who has just that must be denied a replace. */
    public void testThePrivilegesThatAllowOverwritingADocumentGrantTheReplacingAction() {
        for (String privilege : List.of("create", "index", "write", "all")) {
            assertTrue(privilege, IndexPrivilege.get(privilege).predicate().test(EsqlIntoReplaceDestinationAction.NAME));
        }
        for (String privilege : List.of("create_doc", "delete", "read", "maintenance", "manage", "view_index_metadata")) {
            assertFalse(privilege, IndexPrivilege.get(privilege).predicate().test(EsqlIntoReplaceDestinationAction.NAME));
        }
    }

    public void testThePrivilegesThatDoNotAllowIndexingADocumentDoNotGrantTheAction() {
        for (String privilege : List.of("delete", "read", "maintenance", "manage", "view_index_metadata")) {
            assertFalse(privilege, IndexPrivilege.get(privilege).predicate().test(EsqlIntoDestinationAction.NAME));
        }
    }
}
