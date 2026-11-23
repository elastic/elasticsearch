/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * The loaders of the csv-spec tests are static, so they outlive the cluster of a test class. These tests pin what
 * {@link ResourceLoader#reset()} is for: a class that runs after another one in the same JVM gets a new cluster, which has none of
 * what the loader loaded for the previous one.
 */
public class ResourceLoaderTests extends ESTestCase {

    /**
     * A loader over a cluster that is a set of names. Unloading a name that the cluster does not have fails, as deleting a view that is
     * not there does, and a new cluster is an empty set.
     */
    private static final class LoaderOfACluster extends ResourceLoader<String> {
        final Set<String> cluster = new HashSet<>();
        final List<String> loads = new ArrayList<>();
        boolean failToLoad = false;

        @Override
        protected void load(String resource) {
            if (failToLoad) {
                throw new IllegalStateException("cannot load [" + resource + "]");
            }
            loads.add(resource);
            cluster.add(resource);
        }

        @Override
        protected void unload(String name) {
            if (cluster.remove(name) == false) {
                throw new IllegalStateException("view [" + name + "] not found");
            }
        }

        void startNewCluster() {
            cluster.clear();
        }
    }

    public void testLoadsEachResourceOnce() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.maybeLoad("a", "a");
        loader.maybeLoad("a", "a");
        loader.maybeLoad("b", "b");
        assertThat(loader.loads, equalTo(List.of("a", "b")));
    }

    public void testUnloadAllUnloadsWhatWasLoaded() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.maybeLoad("a", "a");
        loader.maybeLoad("b", "b");
        loader.unloadAll();
        assertThat(loader.cluster.isEmpty(), equalTo(true));
        loader.unloadAll();
    }

    /** What happened in the CI without a reset: the first unload of a class in a new cluster failed, and so did every one after it. */
    public void testWithoutAResetANewClusterFailsEveryUnloadAll() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.maybeLoad("view", "view");
        loader.startNewCluster();

        for (int i = 0; i < 3; i++) {
            IllegalStateException e = expectThrows(IllegalStateException.class, loader::unloadAll);
            assertThat(e.getMessage(), containsString("view [view] not found"));
        }
    }

    public void testResetMakesUnloadAllLeaveTheNewClusterAlone() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.maybeLoad("view", "view");
        loader.startNewCluster();
        loader.reset();

        loader.unloadAll();
        assertThat(loader.cluster.isEmpty(), equalTo(true));
    }

    public void testResetMakesTheNewClusterGetTheResourceAgain() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.maybeLoad("index", "index");
        loader.startNewCluster();

        loader.maybeLoad("index", "index");
        assertThat("without a reset the new cluster would never get it", loader.cluster.isEmpty(), equalTo(true));

        loader.reset();
        loader.maybeLoad("index", "index");
        assertThat(loader.cluster, hasItem("index"));
        assertThat(loader.loads, equalTo(List.of("index", "index")));
    }

    public void testResetForgetsAFailureOfThePreviousCluster() {
        LoaderOfACluster loader = new LoaderOfACluster();
        loader.failToLoad = true;
        RuntimeException e = expectThrows(RuntimeException.class, () -> loader.maybeLoad("a", "a"));
        assertThat(e.getMessage(), containsString("Resource loading failure"));
        expectThrows(RuntimeException.class, loader::ensureNoFailures);

        loader.failToLoad = false;
        loader.reset();
        loader.ensureNoFailures();
        loader.maybeLoad("a", "a");
        assertThat(loader.cluster, hasItem("a"));
    }

    public void testAMissingResourceIsAnError() {
        LoaderOfACluster loader = new LoaderOfACluster();
        AssertionError e = expectThrows(AssertionError.class, () -> loader.maybeLoad("a", null));
        assertThat(e.getMessage(), containsString("Resource [a] is not found!"));
    }
}
