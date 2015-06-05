/**
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.assistedinject;

import org.elasticsearch.common.inject.TypeLiteral;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A list of {@link TypeLiteral}s to match an injectable Constructor's assited
 * parameter types to the corresponding factory method.
 *
 * @author jmourits@google.com (Jerome Mourits)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class ParameterListKey {

    private final List<Type> paramList;

    public ParameterListKey(List<Type> paramList) {
        this.paramList = new ArrayList<>(paramList);
    }

    public ParameterListKey(Type[] types) {
        this(Arrays.asList(types));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof ParameterListKey)) {
            return false;
        }
        ParameterListKey other = (ParameterListKey) o;
        return paramList.equals(other.paramList);
    }

    @Override
    public int hashCode() {
        return paramList.hashCode();
    }

    @Override
    public String toString() {
        return paramList.toString();
    }
}