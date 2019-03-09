/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * A ConditionalClusterPrivilege is a composition of a {@link ClusterPrivilege} (that determines which actions may be executed)
 * with a {@link Predicate} for a {@link TransportRequest} (that determines which requests may be executed).
 * The a given execution of an action is considered to be permitted if both the action and the request are permitted.
 */
public interface ConditionalClusterPrivilege extends NamedWriteable, ToXContentFragment {

    /**
     * The category under which this privilege should be rendered when output as XContent.
     */
    Category getCategory();

    /**
     * The action-level privilege that is required by this conditional privilege.
     */
    ClusterPrivilege getPrivilege();

    /**
     * The request-level privilege (as a {@link Predicate}) that is required by this conditional privilege.
     */
    Predicate<TransportRequest> getRequestPredicate();

    /**
     * A {@link ConditionalClusterPrivilege} should generate a fragment of {@code XContent}, which consists of
     * a single field name, followed by its value (which may be an object, an array, or a simple value).
     */
    @Override
    XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Categories exist for to segment privileges for the purposes of rendering to XContent.
     * {@link ConditionalClusterPrivileges#toXContent(XContentBuilder, Params, Collection)} builds one XContent
     * object for a collection of {@link ConditionalClusterPrivilege} instances, with the top level fields built
     * from the categories.
     */
    enum Category {
        APPLICATION(new ParseField("application"));

        public final ParseField field;

        Category(ParseField field) {
            this.field = field;
        }
    }
}
