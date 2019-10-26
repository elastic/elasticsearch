/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents global privileges. "Global Privilege" is a mantra for granular
 * generic cluster privileges. These privileges are organized into categories.
 * Elasticsearch defines the set of categories. Under each category there are
 * operations that are under the clients jurisdiction. The privilege is hence
 * defined under an operation under a category.
 */
public final class GlobalPrivileges implements ToXContentObject {

    // When categories change, adapting this field should suffice. Categories are NOT
    // opaque "named_objects", we wish to maintain control over these namespaces
    public static final List<String> CATEGORIES = Collections.singletonList("application");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GlobalPrivileges, Void> PARSER = new ConstructingObjectParser<>("global_category_privileges",
            false, constructorObjects -> {
                // ignore_unknown_fields is irrelevant here anyway, but let's keep it to false
                // because this conveys strictness (woop woop)
                return new GlobalPrivileges((Collection<GlobalOperationPrivilege>) constructorObjects[0]);
            });

    static {
        for (final String category : CATEGORIES) {
            PARSER.declareNamedObjects(optionalConstructorArg(),
                    (parser, context, operation) -> GlobalOperationPrivilege.fromXContent(category, operation, parser),
                    new ParseField(category));
        }
    }

    private final Set<? extends GlobalOperationPrivilege> privileges;
    // same data as in privileges but broken down by categories; internally, it is
    // easier to work with this structure
    private final Map<String, List<GlobalOperationPrivilege>> privilegesByCategoryMap;

    /**
     * Constructs global privileges by bundling the set of privileges.
     * 
     * @param privileges
     *            The privileges under a category and for an operation in that category.
     */
    public GlobalPrivileges(Collection<? extends GlobalOperationPrivilege> privileges) {
        if (privileges == null || privileges.isEmpty()) {
            throw new IllegalArgumentException("Privileges cannot be empty or null");
        }
        // duplicates are just ignored
        this.privileges = Set.copyOf(Objects.requireNonNull(privileges));
        this.privilegesByCategoryMap = Collections
                .unmodifiableMap(this.privileges.stream().collect(Collectors.groupingBy(GlobalOperationPrivilege::getCategory)));
        for (final Map.Entry<String, List<GlobalOperationPrivilege>> privilegesByCategory : privilegesByCategoryMap.entrySet()) {
            // all operations for a specific category
            final Set<String> allOperations = privilegesByCategory.getValue().stream().map(p -> p.getOperation())
                    .collect(Collectors.toSet());
            if (allOperations.size() != privilegesByCategory.getValue().size()) {
                throw new IllegalArgumentException("Different privileges for the same category and operation are not permitted");
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (final Map.Entry<String, List<GlobalOperationPrivilege>> privilegesByCategory : this.privilegesByCategoryMap.entrySet()) {
            builder.startObject(privilegesByCategory.getKey());
            for (final GlobalOperationPrivilege privilege : privilegesByCategory.getValue()) {
                builder.field(privilege.getOperation(), privilege.getRaw());
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    public static GlobalPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Set<? extends GlobalOperationPrivilege> getPrivileges() {
        return privileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final GlobalPrivileges that = (GlobalPrivileges) o;
        return privileges.equals(that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges);
    }

}
