/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents generic global cluster privileges that can be scoped by categories
 * and then further by operations. The privilege's syntactic and semantic
 * meaning is specific to each category and operation; there is no general
 * definition template. It is not permitted to define different privileges under
 * the same category and operation.
 */
public class GlobalOperationPrivilege {

    private final String category;
    private final String operation;
    private final Map<String, Object> privilege;

    /**
     * Constructs privileges under a specific {@code category} and for some
     * {@code operation}. The privilege definition is flexible, it is a {@code Map},
     * and the semantics is bound to the {@code category} and {@code operation}.
     *
     * @param category
     *            The category of the privilege.
     * @param operation
     *            The operation of the privilege.
     * @param privilege
     *            The privilege definition.
     */
    public GlobalOperationPrivilege(String category, String operation, Map<String, Object> privilege) {
        this.category = Objects.requireNonNull(category);
        this.operation = Objects.requireNonNull(operation);
        if (privilege == null || privilege.isEmpty()) {
            throw new IllegalArgumentException("privileges cannot be empty or null");
        }
        this.privilege = Collections.unmodifiableMap(privilege);
    }

    public String getCategory() {
        return category;
    }

    public String getOperation() {
        return operation;
    }

    public Map<String, Object> getRaw() {
        return privilege;
    }

    public static GlobalOperationPrivilege fromXContent(String category, String operation, XContentParser parser) throws IOException {
        // parser is still placed on the field name, advance to next token (field value)
        assert parser.currentToken().equals(XContentParser.Token.FIELD_NAME);
        parser.nextToken();
        return new GlobalOperationPrivilege(category, operation, parser.map());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (false == (o instanceof GlobalOperationPrivilege)) {
            return false;
        }
        final GlobalOperationPrivilege that = (GlobalOperationPrivilege) o;
        return category.equals(that.category) && operation.equals(that.operation) && privilege.equals(that.privilege);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, operation, privilege);
    }

}
