/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.test.ESTestCase;

import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;

public class DependencySorterTests extends ESTestCase {


    public void testFindDependencies_GivenNoDependencies() {
        List<TransformConfig> transforms = new ArrayList<>();
        List<TransformConfig> deps = DependencySorter.findDependencies("metricField", transforms);
        assertEquals(0, deps.size());
    }


    public void testFindDependencies_Given1Dependency() {
        List<TransformConfig> transforms = new ArrayList<>();

        List<String> inputs = Arrays.asList("ina", "inb");
        List<String> outputs = Arrays.asList("ab");
        TransformConfig concat = createConcatTransform(inputs, outputs);
        transforms.add(concat);

        List<String> inputs2 = Arrays.asList("inc", "ind");
        List<String> outputs2 = Arrays.asList("cd");
        TransformConfig concat2 = createConcatTransform(inputs2, outputs2);
        transforms.add(concat2);


        List<TransformConfig> deps = DependencySorter.findDependencies("cd", transforms);
        assertEquals(1, deps.size());
        assertEquals(deps.get(0), concat2);
    }


    public void testFindDependencies_Given2Dependencies() {
        List<TransformConfig> transforms = new ArrayList<>();

        List<String> inputs = Arrays.asList("ina", "inb");
        List<String> outputs = Arrays.asList("ab");
        TransformConfig concat = createConcatTransform(inputs, outputs);
        transforms.add(concat);

        List<String> inputs2 = Arrays.asList("inc", "ind");
        List<String> outputs2 = Arrays.asList("cd");
        TransformConfig concat2 = createConcatTransform(inputs2, outputs2);
        transforms.add(concat2);


        List<TransformConfig> deps = DependencySorter.findDependencies(Arrays.asList("cd", "ab"),
                transforms);
        assertEquals(2, deps.size());
        assertTrue(deps.contains(concat));
        assertTrue(deps.contains(concat2));
    }


    public void testFindDependencies_GivenChainOfDependencies() {
        List<TransformConfig> transforms = new ArrayList<>();

        List<String> inputs = Arrays.asList("ina", "inb");
        List<String> outputs = Arrays.asList("ab");
        TransformConfig concat = createConcatTransform(inputs, outputs);
        transforms.add(concat);

        List<String> inputs2 = Arrays.asList("ab", "inc");
        List<String> outputs2 = Arrays.asList("abc");
        TransformConfig dependentConcat = createConcatTransform(inputs2, outputs2);
        transforms.add(dependentConcat);

        List<TransformConfig> deps = DependencySorter.findDependencies("abc",
                transforms);
        assertEquals(2, deps.size());
        assertEquals(concat, deps.get(0));
        assertEquals(dependentConcat, deps.get(1));
    }

    /**
     * 2 separate inputs with chain of dependencies one of which is shared
     */

    public void testFindDependencies_Given2ChainsAndSharedDependencys() {
        List<TransformConfig> transforms = new ArrayList<>();

        List<String> inputs2 = Arrays.asList("ab", "inc");
        List<String> outputs2 = Arrays.asList("abc");
        TransformConfig dependentConcat1 = createConcatTransform(inputs2, outputs2);
        transforms.add(dependentConcat1);

        List<String> inputs3 = Arrays.asList("ab", "ind");
        List<String> outputs3 = Arrays.asList("abd");
        TransformConfig dependentConcat2 = createConcatTransform(inputs3, outputs3);
        transforms.add(dependentConcat2);

        List<String> inputs = Arrays.asList("ina", "inb");
        List<String> outputs = Arrays.asList("ab");
        TransformConfig concat = createConcatTransform(inputs, outputs);
        transforms.add(concat);

        List<TransformConfig> deps = DependencySorter.findDependencies(Arrays.asList("abc", "abd"),
                transforms);
        assertEquals(3, deps.size());
        assertEquals(concat, deps.get(0));
        assertEquals(dependentConcat1, deps.get(1));
        assertEquals(dependentConcat2, deps.get(2));
    }


    public void testSortByDependency_NoDependencies() {
        List<TransformConfig> transforms = new ArrayList<>();

        TransformConfig concat = createConcatTransform(Arrays.asList("ina", "inb"),
                Arrays.asList("ab"));
        transforms.add(concat);

        TransformConfig hrd1 = createHrdTransform(Arrays.asList("dns"),
                Arrays.asList("subdomain", "hrd"));
        transforms.add(hrd1);


        TransformConfig hrd2 = createHrdTransform(Arrays.asList("dns2"),
                Arrays.asList("subdomain"));
        transforms.add(hrd2);

        List<TransformConfig> orderedDeps = DependencySorter.sortByDependency(transforms);

        assertEquals(transforms.size(), orderedDeps.size());
    }


    public void testSortByDependency_SingleChain() {
        List<TransformConfig> transforms = new ArrayList<>();

        // Chain of 3 dependencies
        TransformConfig chain1Hrd = createHrdTransform(Arrays.asList("ab"),
                Arrays.asList("subdomain", "hrd"));
        transforms.add(chain1Hrd);

        TransformConfig chain1Concat = createConcatTransform(Arrays.asList("ina", "inb"),
                Arrays.asList("ab"));
        transforms.add(chain1Concat);

        TransformConfig chain1Concat2 = createConcatTransform(Arrays.asList("subdomain", "port"),
                Arrays.asList());
        transforms.add(chain1Concat2);

        List<TransformConfig> orderedDeps = DependencySorter.sortByDependency(transforms);

        assertEquals(transforms.size(), orderedDeps.size());

        int chain1ConcatIndex = orderedDeps.indexOf(chain1Concat);
        assertTrue(chain1ConcatIndex == 0);
        int chain1HrdIndex = orderedDeps.indexOf(chain1Hrd);
        assertTrue(chain1HrdIndex == 1);
        int chain1Concat2Index = orderedDeps.indexOf(chain1Concat2);
        assertTrue(chain1Concat2Index == 2);

        assertTrue(chain1ConcatIndex < chain1HrdIndex);
        assertTrue(chain1HrdIndex < chain1Concat2Index);
    }


    public void testSortByDependency_3ChainsInOrder() {
        List<TransformConfig> transforms = new ArrayList<>();

        // Chain of 1
        TransformConfig noChainHrd = createHrdTransform(Arrays.asList("dns"),
                Arrays.asList("subdomain"));
        transforms.add(noChainHrd);

        // Chain of 2 dependencies
        TransformConfig chain1Concat = createConcatTransform(Arrays.asList("ina", "inb"),
                Arrays.asList("ab"));
        transforms.add(chain1Concat);

        TransformConfig chain1Hrd = createHrdTransform(Arrays.asList("ab"),
                Arrays.asList("subdomain", "hrd"));
        transforms.add(chain1Hrd);

        // Chain of 2 dependencies
        TransformConfig chain2Concat2 = createConcatTransform(Arrays.asList("cd", "ine"),
                Arrays.asList("cde"));
        transforms.add(chain2Concat2);

        TransformConfig chain2Concat = createConcatTransform(Arrays.asList("inc", "ind"),
                Arrays.asList("cd"));
        transforms.add(chain2Concat);


        List<TransformConfig> orderedDeps = DependencySorter.sortByDependency(transforms);

        assertEquals(transforms.size(), orderedDeps.size());

        int chain1ConcatIndex = orderedDeps.indexOf(chain1Concat);
        assertTrue(chain1ConcatIndex >= 0);
        int chain1HrdIndex = orderedDeps.indexOf(chain1Hrd);
        assertTrue(chain1HrdIndex >= 1);
        assertTrue(chain1ConcatIndex < chain1HrdIndex);

        int chain2ConcatIndex = orderedDeps.indexOf(chain2Concat);
        assertTrue(chain2ConcatIndex >= 0);
        int chain2Concat2Index = orderedDeps.indexOf(chain2Concat2);
        assertTrue(chain2Concat2Index >= 1);
        assertTrue(chain2ConcatIndex < chain2Concat2Index);
    }


    public void testSortByDependency_3ChainsOutOfOrder() {
        List<TransformConfig> transforms = new ArrayList<>();

        TransformConfig chain1Hrd = createHrdTransform(Arrays.asList("ab"),
                Arrays.asList("subdomain", "hrd"));
        transforms.add(chain1Hrd);

        TransformConfig chain2Concat2 = createConcatTransform(Arrays.asList("cd", "ine"),
                Arrays.asList("cde"));
        transforms.add(chain2Concat2);

        TransformConfig chain1Concat = createConcatTransform(Arrays.asList("ina", "inb"),
                Arrays.asList("ab"));
        transforms.add(chain1Concat);

        TransformConfig noChainHrd = createHrdTransform(Arrays.asList("dns"),
                Arrays.asList("subdomain"));
        transforms.add(noChainHrd);

        TransformConfig chain2Concat = createConcatTransform(Arrays.asList("inc", "ind"),
                Arrays.asList("cd"));
        transforms.add(chain2Concat);

        List<TransformConfig> orderedDeps = DependencySorter.sortByDependency(transforms);

        assertEquals(transforms.size(), orderedDeps.size());

        int chain1ConcatIndex = orderedDeps.indexOf(chain1Concat);
        assertTrue(chain1ConcatIndex >= 0);
        int chain1HrdIndex = orderedDeps.indexOf(chain1Hrd);
        assertTrue(chain1HrdIndex >= 0);
        assertTrue(chain1ConcatIndex < chain1HrdIndex);

        int chain2ConcatIndex = orderedDeps.indexOf(chain2Concat);
        assertTrue(chain2ConcatIndex >= 0);
        int chain2Concat2Index = orderedDeps.indexOf(chain2Concat2);
        assertTrue(chain2Concat2Index >= 0);
        assertTrue(chain2ConcatIndex < chain2Concat2Index);
    }

    private TransformConfig createConcatTransform(List<String> inputs, List<String> outputs) {
        TransformConfig concat = new TransformConfig(TransformType.CONCAT.prettyName());
        concat.setInputs(inputs);
        concat.setOutputs(outputs);
        return concat;
    }

    private TransformConfig createHrdTransform(List<String> inputs, List<String> outputs) {
        TransformConfig concat = new TransformConfig(TransformType.DOMAIN_SPLIT.prettyName());
        concat.setInputs(inputs);
        concat.setOutputs(outputs);
        return concat;
    }
}
