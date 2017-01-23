/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;

/**
 * Transform inputs and outputs can be chained together this class provides
 * methods for finding the chains of dependencies is a list of transforms. The
 * results are ordered list of transforms that should be executed in order
 * starting at index 0
 */
public final class DependencySorter {
    /**
     * Hide public constructor
     */
    private DependencySorter() {

    }

    /**
     * For the input field get the chain of transforms that must be executed to
     * get that field. The returned list is ordered so that the ones at the end
     * of the list are dependent on those at the beginning.
     * <p>
     * Note if there is a circular dependency in the list of transforms this
     * will cause a stack overflow. Check with
     * {@linkplain org.elasticsearch.xpack.ml.job.config.transform.verification.TransformConfigsVerifier#checkForCircularDependencies(List)}
     * first.
     *
     * @return List of transforms ordered by dependencies
     */
    public static List<TransformConfig> findDependencies(String input, List<TransformConfig> transforms) {
        return findDependencies(Arrays.asList(input), transforms);
    }

    /**
     * For the list of input fields get the chain of transforms that must be
     * executed to get those fields. The returned list is ordered so that the
     * ones at the end of the list are dependent on those at the beginning
     * <p>
     * Note if there is a circular dependency in the list of transforms this
     * will cause a stack overflow. Check with
     * {@linkplain org.elasticsearch.xpack.ml.job.config.transform.verification.TransformConfigsVerifier#checkForCircularDependencies(List)}
     * first.
     *
     * @return List of transforms ordered by dependencies
     */
    public static List<TransformConfig> findDependencies(List<String> inputs, List<TransformConfig> transforms) {
        List<TransformConfig> dependencies = new LinkedList<>();

        ListIterator<TransformConfig> itr = transforms.listIterator();
        while (itr.hasNext()) {
            TransformConfig tc = itr.next();
            for (String input : inputs) {
                if (tc.getOutputs().contains(input)) {
                    findDependenciesRecursive(tc, transforms, dependencies);
                }
            }

        }
        return dependencies;
    }

    /**
     * Recursively find the transform dependencies and add them to the
     * dependency list
     *
     */
    private static void findDependenciesRecursive(TransformConfig transform, List<TransformConfig> transforms,
            List<TransformConfig> dependencies) {
        int index = dependencies.indexOf(transform);
        if (index >= 0) {
            return;
        }

        ListIterator<TransformConfig> itr = transforms.listIterator();
        while (itr.hasNext()) {
            TransformConfig tc = itr.next();

            for (String input : transform.getInputs()) {
                if (tc.getOutputs().contains(input)) {
                    findDependenciesRecursive(tc, transforms, dependencies);
                }
            }
        }

        dependencies.add(transform);
    }

    /**
     * Return an ordered list of transforms (the same size as the input list)
     * that sorted in terms of dependencies.
     * <p>
     * Note if there is a circular dependency in the list of transforms this
     * will cause a stack overflow. Check with
     * {@linkplain org.elasticsearch.xpack.ml.job.config.transform.verification.TransformConfigsVerifier#checkForCircularDependencies(List)}
     * first.
     *
     * @return List of transforms ordered by dependencies
     */
    public static List<TransformConfig> sortByDependency(List<TransformConfig> transforms) {
        List<TransformConfig> orderedDependencies = new LinkedList<>();
        List<TransformConfig> transformsCopy = new LinkedList<>(transforms);

        transformsCopy = orderDependenciesRecursive(transformsCopy, orderedDependencies);
        while (transformsCopy.isEmpty() == false) {
            transformsCopy = orderDependenciesRecursive(transformsCopy, orderedDependencies);
        }

        return orderedDependencies;
    }

    /**
     * Find the dependencies of the head of the <code>transforms</code> list
     * adding them to the <code>dependencies</code> list. The returned list is a
     * copy of the input <code>transforms</code> with the dependent transforms
     * (i.e. those that have been ordered and add to <code>dependencies</code>)
     * removed.
     * <p>
     * In the case where the input <code>transforms</code> list contains
     * multiple chains of dependencies this function should be called multiple
     * times using its return value as the input <code>transforms</code>
     * parameter
     * <p>
     * To avoid concurrent modification of the transforms list a new copy is
     * made for each recursive call and a new modified list returned
     *
     * @param dependencies
     *            Transforms are added to this list
     * @return As transforms are moved from <code>transforms</code> to
     *         <code>dependencies</code> this list is a new copy of the
     *         <code>transforms</code> input with the moved transforms removed.
     */
    private static List<TransformConfig> orderDependenciesRecursive(List<TransformConfig> transforms, List<TransformConfig> dependencies) {
        if (transforms.isEmpty()) {
            return transforms;
        }

        ListIterator<TransformConfig> itr = transforms.listIterator();
        TransformConfig transform = itr.next();
        itr.remove();

        int index = dependencies.indexOf(transform);
        if (index >= 0) {
            return transforms;
        }

        while (itr.hasNext()) {
            TransformConfig tc = itr.next();

            for (String input : transform.getInputs()) {
                if (tc.getOutputs().contains(input)) {
                    transforms = orderDependenciesRecursive(new LinkedList<TransformConfig>(transforms), dependencies);

                    itr = transforms.listIterator();
                }
            }
        }

        dependencies.add(transform);
        return transforms;
    }

}
