/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.util.yaml.snakeyaml.introspector;

import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;

import java.beans.PropertyDescriptor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MethodProperty extends Property {
    private final PropertyDescriptor property;

    public MethodProperty(PropertyDescriptor property) {
        super(property.getName(), property.getPropertyType());
        this.property = property;
    }

    @Override
    public void set(Object object, Object value) throws Exception {
        property.getWriteMethod().invoke(object, value);
    }

    @Override
    public Type[] getActualTypeArguments() {
        if (List.class.isAssignableFrom(property.getPropertyType())
                || Set.class.isAssignableFrom(property.getPropertyType())
                || Map.class.isAssignableFrom(property.getPropertyType())) {
            if (property.getReadMethod().getGenericReturnType() instanceof ParameterizedType) {
                ParameterizedType grt = (ParameterizedType) property.getReadMethod()
                        .getGenericReturnType();
                return grt.getActualTypeArguments();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public Object get(Object object) {
        try {
            return property.getReadMethod().invoke(object);
        } catch (Exception e) {
            throw new YAMLException("Unable to find getter for property '" + property.getName()
                    + "' on object " + object + ":" + e);
        }
    }
}