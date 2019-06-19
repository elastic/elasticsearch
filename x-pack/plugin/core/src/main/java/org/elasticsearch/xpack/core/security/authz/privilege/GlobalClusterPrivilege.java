/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * A GlobalClusterPrivilege is a {@link ConditionalClusterPrivilege} that can be serialized / rendered as `XContent`.
 */
public abstract class GlobalClusterPrivilege extends ConditionalClusterPrivilege implements NamedWriteable, ToXContentFragment {

    public GlobalClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    public GlobalClusterPrivilege(String name, Automaton automaton) {
        this(Collections.singleton(name), automaton);
    }

    public GlobalClusterPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }

    /**
     * The category under which this privilege should be rendered when output as XContent.
     */
    public abstract Category getCategory();

    /**
     * A {@link GlobalClusterPrivilege} should generate a fragment of {@code XContent}, which consists of
     * a single field name, followed by its value (which may be an object, an array, or a simple value).
     */
    @Override
    public abstract XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Categories exist for to segment privileges for the purposes of rendering to XContent.
     * {@link GlobalClusterPrivileges#toXContent(XContentBuilder, Params, Collection)} builds one XContent
     * object for a collection of {@link GlobalClusterPrivilege} instances, with the top level fields built
     * from the categories.
     */
    public enum Category {
        APPLICATION(new ParseField("application"));

        public final ParseField field;

        Category(ParseField field) {
            this.field = field;
        }
    }
}
