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

package org.elasticsearch.client.security.support.expressiondsl.fields;

import org.elasticsearch.common.Strings;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for field expression.<br>
 * Example usage:
 *
 * <pre>
 * {@code
 * UsernameFieldExpression usernameFieldExpression = FieldExpressionBuilder.builder(UsernameFieldExpression.class)
       .addValue("es-admin")
       .addValue("es-system")
       .build());
 * }
 * </pre>
 */
public final class FieldExpressionBuilder<T extends FieldExpressionBase> {
    private Constructor<T> ctor;
    private Class<T> clazz;
    private String key;
    private List<Object> elements = new ArrayList<>();

    private FieldExpressionBuilder(Class<T> clazz) {
        try {
            if (clazz.isAssignableFrom(MetadataFieldExpression.class)) {
                this.ctor = clazz.getConstructor(String.class, Object[].class);
            } else {
                this.ctor = clazz.getConstructor(Object[].class);
            }
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
        this.clazz = clazz;
    }

    public static <T extends FieldExpressionBase> FieldExpressionBuilder<T> builder(Class<T> clazz) {
        return new FieldExpressionBuilder<>(clazz);
    }

    public FieldExpressionBuilder<T> withKey(String key) {
        assert Strings.hasLength(key) : "metadata key cannot be null or empty";
        assert MetadataFieldExpression.class.isAssignableFrom(
                clazz) : "metadat key can only be provided when building MetadataFieldExpression";
        this.key = key;
        return this;
    }
    
    public FieldExpressionBuilder<T> addValue(Object value) {
        elements.add(value);
        return this;
    }

    public T build() {
        try {
            if (MetadataFieldExpression.class.isAssignableFrom(clazz)) {
                return ctor.newInstance(key, elements.toArray());
            } else {
                return ctor.newInstance(new Object[] { elements.toArray() });
            }
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
