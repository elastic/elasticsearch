/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.transport.TransportRequest;

import java.util.function.Predicate;

public interface ConditionalClusterPrivilege extends NamedWriteable, ToXContentFragment {
    Category getCategory();

    ClusterPrivilege getPrivilege();

    Predicate<TransportRequest> getRequestPredicate();

    enum Category {
        APPLICATION(new ParseField("application"));

        public final ParseField field;

        Category(ParseField field) {
            this.field = field;
        }
    }
}
