/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
