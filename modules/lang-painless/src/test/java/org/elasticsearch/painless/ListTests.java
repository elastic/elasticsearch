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

package org.elasticsearch.painless;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/** Tests for working with lists. */
public class ListTests extends ArrayLikeObjectTestCase {
    @Override
    protected String declType(String valueType) {
        return "List";
    }

    @Override
    protected String valueCtorCall(String valueType, int size) {
        String[] fill = new String[size];
        Arrays.fill(fill, fillValue(valueType));
        return "[" + String.join(",", fill) + "]";
    }

    private String fillValue(String valueType) {
        switch (valueType) {
        case "int":    return "0";
        case "long":   return "0L";
        case "short":  return "(short) 0";
        case "byte":   return "(byte) 0";
        case "float":  return "0.0f";
        case "double": return "0.0"; // Double is implicit for decimal constants
        default:       return null;
        }
    }

    @Override
    protected Matcher<String> outOfBoundsExceptionMessageMatcher(int index, int size) {
        if ("1.8".equals(Runtime.class.getPackage().getSpecificationVersion())) {
            if (index > size) {
                return equalTo("Index: " + index + ", Size: " + size);
            }
            return equalTo(Integer.toString(index));
        } else {
            // This exception is locale dependent so we attempt to reproduce it
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                list.add(new Object());
            }
            Exception e = expectThrows(IndexOutOfBoundsException.class, () -> list.get(index));
            return equalTo(e.getMessage());
        }
    }

}
