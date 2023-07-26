/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Utility methods for dealing with nested mappers
 */
public final class NestedUtils {

    private NestedUtils() {}

    /**
     * Partition a set of input objects by the children of a specific nested scope
     *
     * The returned map will contain an entry for all children, even if some of them
     * are empty in the inputs.
     *
     * All children, and all input paths, must begin with the scope.  Both children
     * and inputs should be in sorted order.
     *
     * @param scope         the nested scope to base partitions on
     * @param children      the immediate children of the nested scope
     * @param inputs        a set of inputs to partition
     * @param pathFunction  a function to retrieve a path for each input
     * @param <T>           the type of the inputs
     * @return              a map of nested paths to lists of inputs
     */
    public static <T> Map<String, List<T>> partitionByChildren(
        String scope,
        List<String> children,
        List<T> inputs,
        Function<T, String> pathFunction
    ) {
        // No immediate nested children, so we can shortcut and just return all inputs
        // under the current scope
        if (children.isEmpty()) {
            return Map.of(scope, inputs);
        }

        // Set up the output map, with one entry for the current scope and one for each
        // of its children
        Map<String, List<T>> output = new HashMap<>();
        output.put(scope, new ArrayList<>());
        for (String child : children) {
            output.put(child, new ArrayList<>());
        }

        // No inputs, so we can return the output map with all entries empty
        if (inputs.isEmpty()) {
            return output;
        }

        Iterator<String> childrenIterator = children.iterator();
        String currentChild = childrenIterator.next();
        Iterator<T> inputIterator = inputs.iterator();
        T currentInput = inputIterator.next();
        String currentInputName = pathFunction.apply(currentInput);
        assert currentInputName.startsWith(scope);

        // Iterate through all the children
        while (currentChild != null) {
            if (currentInputName.compareTo(currentChild) < 0) {
                // If we sort before the current child, then we sit in the parent scope
                output.get(scope).add(currentInput);
                if (inputIterator.hasNext() == false) {
                    return output;
                }
                currentInput = inputIterator.next();
                currentInputName = pathFunction.apply(currentInput);
                assert currentInputName.startsWith(scope);
            } else if (currentInputName.startsWith(currentChild + ".")) {
                // If this input sits under the current child, add it to that child scope
                // and then get the next input
                output.get(currentChild).add(currentInput);
                if (inputIterator.hasNext() == false) {
                    // return if no more inputs
                    return output;
                }
                currentInput = inputIterator.next();
                currentInputName = pathFunction.apply(currentInput);
                assert currentInputName.startsWith(scope);
            } else {
                // If there are no more children then skip to filling up the parent scope again
                if (childrenIterator.hasNext() == false) {
                    break;
                }
                // Move to the next child
                currentChild = childrenIterator.next();
                if (currentChild == null || currentInputName.compareTo(currentChild) < 0) {
                    // If we still sort before the next child, then add to the parent scope
                    // and move to the next input
                    output.get(scope).add(currentInput);
                    if (inputIterator.hasNext() == false) {
                        // if no more inputs then return
                        return output;
                    }
                    currentInput = inputIterator.next();
                    currentInputName = pathFunction.apply(currentInput);
                    assert currentInputName.startsWith(scope);
                }
            }
        }
        output.get(scope).add(currentInput);

        // if there are inputs left, then they all sort after the last child but
        // are not contained by them, so just add them all to the parent scope
        while (inputIterator.hasNext()) {
            currentInput = inputIterator.next();
            currentInputName = pathFunction.apply(currentInput);
            assert currentInputName.startsWith(scope);
            output.get(scope).add(currentInput);
        }
        return output;
    }

}
