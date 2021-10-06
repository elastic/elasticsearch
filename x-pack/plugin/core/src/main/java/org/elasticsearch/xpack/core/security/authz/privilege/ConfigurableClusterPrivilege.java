/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

/**
 * A ConfigurableClusterPrivilege is a form of {@link ClusterPrivilege} that can be configured by an Elasticsearch security administrator
 * within a {@link org.elasticsearch.xpack.core.security.authz.RoleDescriptor}.
 */
public interface ConfigurableClusterPrivilege extends NamedWriteable, ToXContentFragment, ClusterPrivilege {

    /**
     * The category under which this privilege should be rendered when output as XContent.
     */
    Category getCategory();

    /**
     * A {@link ConfigurableClusterPrivilege} should generate a fragment of {@code XContent}, which consists of
     * a single field name, followed by its value (which may be an object, an array, or a simple value).
     */
    @Override
    XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Categories exist for to segment privileges for the purposes of rendering to XContent.
     * {@link ConfigurableClusterPrivileges#toXContent(XContentBuilder, Params, Collection)} builds one XContent
     * object for a collection of {@link ConfigurableClusterPrivilege} instances, with the top level fields built
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
