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
import org.elasticsearch.client.security.support.expressiondsl.fields.DnFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldExpressionBuilder;
import org.elasticsearch.client.security.support.expressiondsl.fields.GroupsFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.MetadataFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.UsernameFieldExpression;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for composite role mapper expressions.<br>
 * Example usage:
 * <pre>
 * {@code
 * final AllExpression allExpression = CompositeExpressionBuilder.builder(AllExpression.class)
                .addExpression(CompositeExpressionBuilder.builder(AnyExpression.class)
                        .addExpression(FieldExpressionBuilder.builder(DnFieldExpression.class)
                                .addValue("*,ou=admin,dc=example,dc=com")
                                .build())
                        .addExpression(FieldExpressionBuilder.builder(UsernameFieldExpression.class)
                                .addValue("es-admin").addValue("es-system")
                                .build())
                        .build())
                .addExpression(FieldExpressionBuilder.builder(GroupsFieldExpression.class)
                        .addValue("cn=people,dc=example,dc=com")
                        .build())
                .addExpression(new ExceptExpression(FieldExpressionBuilder.builder(MetadataFieldExpression.class)
                                            .withKey("metadata.terminated_date")
                                            .addValue(new Date())
                                            .build()))
                .build();
 * }
 * </pre>
 */
public final class CompositeExpressionBuilder<T extends CompositeRoleMapperExpressionBase> {
    private Constructor<T> ctor;
    private List<RoleMapperExpression> elements = new ArrayList<>();

    private CompositeExpressionBuilder(Class<T> clazz) {
        try {
            this.ctor = clazz.getConstructor(RoleMapperExpression[].class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends CompositeRoleMapperExpressionBase> CompositeExpressionBuilder<T> builder(Class<T> clazz) {
        return new CompositeExpressionBuilder<>(clazz);
    }

    public CompositeExpressionBuilder<T> addExpression(final RoleMapperExpression expression) {
        assert expression != null : "expression cannot be null";
        elements.add(expression);
        return this;
    }

    public T build() {
        try {
            return ctor.newInstance(new Object[] { elements.toArray(new RoleMapperExpression[0]) });
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
