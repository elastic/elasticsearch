/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.info;

import org.elasticsearch.gradle.internal.BwcVersions;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.jvm.toolchain.JavaToolchainSpec;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class BuildParams {
    private static File runtimeJavaHome;
    private static Boolean isRuntimeJavaHomeSet;
    private static List<JavaHome> javaVersions;
    private static JavaVersion minimumCompilerVersion;
    private static JavaVersion minimumRuntimeVersion;
    private static JavaVersion gradleJavaVersion;
    private static JavaVersion runtimeJavaVersion;
    private static Provider<? extends Action<JavaToolchainSpec>> javaToolChainSpec;
    private static String runtimeJavaDetails;
    private static Boolean inFipsJvm;
    private static String gitRevision;
    private static String gitOrigin;
    private static ZonedDateTime buildDate;
    private static String testSeed;
    private static Boolean isCi;
    private static Integer defaultParallel;
    private static Boolean isSnapshotBuild;
    private static Provider<BwcVersions> bwcVersions;

    /**
     * Initialize global build parameters. This method accepts and a initialization function which in turn accepts a
     * {@link MutableBuildParams}. Initialization can be done in "stages", therefore changes override existing values, and values from
     * previous calls to {@link #init(Consumer)} carry forward. In cases where you want to clear existing values
     * {@link MutableBuildParams#reset()} may be used.
     *
     * @param initializer Build parameter initializer
     */
    public static void init(Consumer<MutableBuildParams> initializer) {
        initializer.accept(MutableBuildParams.INSTANCE);
    }

    public static File getRuntimeJavaHome() {
        return value(runtimeJavaHome);
    }

    public static Boolean getIsRuntimeJavaHomeSet() {
        return value(isRuntimeJavaHomeSet);
    }

    public static List<JavaHome> getJavaVersions() {
        return value(javaVersions);
    }

    public static JavaVersion getMinimumCompilerVersion() {
        return value(minimumCompilerVersion);
    }

    public static JavaVersion getMinimumRuntimeVersion() {
        return value(minimumRuntimeVersion);
    }

    public static JavaVersion getGradleJavaVersion() {
        return value(gradleJavaVersion);
    }

    public static JavaVersion getRuntimeJavaVersion() {
        return value(runtimeJavaVersion);
    }

    public static String getRuntimeJavaDetails() {
        return value(runtimeJavaDetails);
    }

    public static Boolean isInFipsJvm() {
        return value(inFipsJvm);
    }

    public static void withFipsEnabledOnly(Task task) {
        task.onlyIf("FIPS mode disabled", task1 -> isInFipsJvm() == false);
    }

    public static String getGitRevision() {
        return value(gitRevision);
    }

    public static String getGitOrigin() {
        return value(gitOrigin);
    }

    public static ZonedDateTime getBuildDate() {
        return value(buildDate);
    }

    public static BwcVersions getBwcVersions() {
        return value(bwcVersions).get();
    }

    public static String getTestSeed() {
        return value(testSeed);
    }

    public static Random getRandom() {
        return new Random(Long.parseUnsignedLong(testSeed.split(":")[0], 16));
    }

    public static Boolean isCi() {
        return value(isCi);
    }

    public static Boolean isGraalVmRuntime() {
        return value(runtimeJavaDetails.toLowerCase().contains("graalvm"));
    }

    public static Integer getDefaultParallel() {
        return value(defaultParallel);
    }

    public static boolean isSnapshotBuild() {
        return value(BuildParams.isSnapshotBuild);
    }

    public static Provider<? extends Action<JavaToolchainSpec>> getJavaToolChainSpec() {
        return javaToolChainSpec;
    }

    private static <T> T value(T object) {
        if (object == null) {
            String callingMethod = Thread.currentThread().getStackTrace()[2].getMethodName();

            throw new IllegalStateException(
                "Build parameter '"
                    + propertyName(callingMethod)
                    + "' has not been initialized.\n"
                    + "Perhaps the plugin responsible for initializing this property has not been applied."
            );
        }

        return object;
    }

    private static String propertyName(String methodName) {
        String propertyName = methodName.startsWith("is") ? methodName.substring("is".length()) : methodName.substring("get".length());
        return propertyName.substring(0, 1).toLowerCase() + propertyName.substring(1);
    }

    public static class MutableBuildParams {
        private static MutableBuildParams INSTANCE = new MutableBuildParams();

        private MutableBuildParams() {}

        /**
         * Resets any existing values from previous initializations.
         */
        public void reset() {
            Arrays.stream(BuildParams.class.getDeclaredFields()).filter(f -> Modifier.isStatic(f.getModifiers())).forEach(f -> {
                try {
                    // Since we are mutating private static fields from a public static inner class we need to suppress
                    // accessibility controls here.
                    f.setAccessible(true);
                    f.set(null, null);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public void setRuntimeJavaHome(File runtimeJavaHome) {
            try {
                BuildParams.runtimeJavaHome = requireNonNull(runtimeJavaHome).getCanonicalFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setIsRuntimeJavaHomeSet(boolean isRutimeJavaHomeSet) {
            BuildParams.isRuntimeJavaHomeSet = isRutimeJavaHomeSet;
        }

        public void setJavaVersions(List<JavaHome> javaVersions) {
            BuildParams.javaVersions = requireNonNull(javaVersions);
        }

        public void setMinimumCompilerVersion(JavaVersion minimumCompilerVersion) {
            BuildParams.minimumCompilerVersion = requireNonNull(minimumCompilerVersion);
        }

        public void setMinimumRuntimeVersion(JavaVersion minimumRuntimeVersion) {
            BuildParams.minimumRuntimeVersion = requireNonNull(minimumRuntimeVersion);
        }

        public void setGradleJavaVersion(JavaVersion gradleJavaVersion) {
            BuildParams.gradleJavaVersion = requireNonNull(gradleJavaVersion);
        }

        public void setRuntimeJavaVersion(JavaVersion runtimeJavaVersion) {
            BuildParams.runtimeJavaVersion = requireNonNull(runtimeJavaVersion);
        }

        public void setRuntimeJavaDetails(String runtimeJavaDetails) {
            BuildParams.runtimeJavaDetails = runtimeJavaDetails;
        }

        public void setInFipsJvm(boolean inFipsJvm) {
            BuildParams.inFipsJvm = inFipsJvm;
        }

        public void setGitRevision(String gitRevision) {
            BuildParams.gitRevision = requireNonNull(gitRevision);
        }

        public void setGitOrigin(String gitOrigin) {
            BuildParams.gitOrigin = requireNonNull(gitOrigin);
        }

        public void setBuildDate(ZonedDateTime buildDate) {
            BuildParams.buildDate = requireNonNull(buildDate);
        }

        public void setTestSeed(String testSeed) {
            BuildParams.testSeed = requireNonNull(testSeed);
        }

        public void setIsCi(boolean isCi) {
            BuildParams.isCi = isCi;
        }

        public void setDefaultParallel(int defaultParallel) {
            BuildParams.defaultParallel = defaultParallel;
        }

        public void setIsSnapshotBuild(final boolean isSnapshotBuild) {
            BuildParams.isSnapshotBuild = isSnapshotBuild;
        }

        public void setBwcVersions(Provider<BwcVersions> bwcVersions) {
            BuildParams.bwcVersions = requireNonNull(bwcVersions);
        }

        public void setJavaToolChainSpec(Provider<? extends Action<JavaToolchainSpec>> javaToolChain) {
            BuildParams.javaToolChainSpec = javaToolChain;
        }
    }
}
