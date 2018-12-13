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

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * An expression that evaluates to <code>true</code> if-and-only-if all its children
 * evaluate to <code>true</code>.
 * An <em>all</em> expression with no children is always <code>true</code>.
 */
public final class AllRoleMapperExpression extends CompositeRoleMapperExpression {

    private AllRoleMapperExpression(String name, RoleMapperExpression[] elements) {
        super(name, elements);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<RoleMapperExpression> elements = new ArrayList<>();

        public Builder addExpression(final RoleMapperExpression expression) {
            assert expression != null : "expression cannot be null";
            elements.add(expression);
            return this;
        }

        public AllRoleMapperExpression build() {
            return new AllRoleMapperExpression(CompositeType.ALL.getName(), elements.toArray(new RoleMapperExpression[0]));
        }
    }
}
