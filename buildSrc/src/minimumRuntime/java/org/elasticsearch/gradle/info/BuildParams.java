package org.elasticsearch.gradle.info;

import org.gradle.api.JavaVersion;

import java.io.File;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class BuildParams {
    private static File compilerJavaHome;
    private static File runtimeJavaHome;
    private static Boolean isRuntimeJavaHomeSet;
    private static List<JavaHome> javaVersions;
    private static JavaVersion minimumCompilerVersion;
    private static JavaVersion minimumRuntimeVersion;
    private static JavaVersion gradleJavaVersion;
    private static JavaVersion compilerJavaVersion;
    private static JavaVersion runtimeJavaVersion;
    private static Boolean inFipsJvm;
    private static String gitRevision;
    private static ZonedDateTime buildDate;
    private static String testSeed;
    private static Boolean isCi;
    private static Boolean isInternal;
    private static Integer defaultParallel;

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

    public static File getCompilerJavaHome() {
        return value(compilerJavaHome);
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

    @ExecutionTime
    public static JavaVersion getCompilerJavaVersion() {
        return value(compilerJavaVersion);
    }

    @ExecutionTime
    public static JavaVersion getRuntimeJavaVersion() {
        return value(runtimeJavaVersion);
    }

    public static Boolean isInFipsJvm() {
        return value(inFipsJvm);
    }

    public static String getGitRevision() {
        return value(gitRevision);
    }

    public static ZonedDateTime getBuildDate() {
        return value(buildDate);
    }

    public static String getTestSeed() {
        return value(testSeed);
    }

    public static Boolean isCi() {
        return value(isCi);
    }

    public static Boolean isInternal() {
        return value(isInternal);
    }

    public static Integer getDefaultParallel() {
        return value(defaultParallel);
    }

    private static <T> T value(T object) {
        if (object == null) {
            String callingMethod = Thread.currentThread().getStackTrace()[2].getMethodName();
            boolean executionTime;
            try {
                executionTime = BuildParams.class.getMethod(callingMethod).getAnnotation(ExecutionTime.class) != null;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }

            String message = "Build parameter '" + propertyName(callingMethod) + "' has not been initialized. ";
            if (executionTime) {
                message += "This property is initialized at execution time, " +
                    "please ensure you are not attempting to access it during project configuration.";
            } else {
                message += "Perhaps the plugin responsible for initializing this property has not been applied.";
            }

            throw new IllegalStateException(message);
        }

        return object;
    }

    private static String propertyName(String methodName) {
        String propertyName = methodName.startsWith("is") ? methodName.substring("is".length()) : methodName.substring("get".length());
        return propertyName.substring(0, 1).toLowerCase() + propertyName.substring(1);
    }

    public static class MutableBuildParams {
        private static MutableBuildParams INSTANCE = new MutableBuildParams();

        private MutableBuildParams() { }

        /**
         * Resets any existing values from previous initializations.
         */
        public void reset() {
            Arrays.stream(BuildParams.class.getDeclaredFields())
                .filter(f -> Modifier.isStatic(f.getModifiers()))
                .forEach(f -> {
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

        public void setCompilerJavaHome(File compilerJavaHome) {
            BuildParams.compilerJavaHome = requireNonNull(compilerJavaHome);
        }

        public void setRuntimeJavaHome(File runtimeJavaHome) {
            BuildParams.runtimeJavaHome = requireNonNull(runtimeJavaHome);
        }

        public void setIsRutimeJavaHomeSet(boolean isRutimeJavaHomeSet) {
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

        public void setCompilerJavaVersion(JavaVersion compilerJavaVersion) {
            BuildParams.compilerJavaVersion = requireNonNull(compilerJavaVersion);
        }

        public void setRuntimeJavaVersion(JavaVersion runtimeJavaVersion) {
            BuildParams.runtimeJavaVersion = requireNonNull(runtimeJavaVersion);
        }

        public void setInFipsJvm(boolean inFipsJvm) {
            BuildParams.inFipsJvm = inFipsJvm;
        }

        public void setGitRevision(String gitRevision) {
            BuildParams.gitRevision = requireNonNull(gitRevision);
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

        public void setIsInternal(Boolean isInternal) {
            BuildParams.isInternal = requireNonNull(isInternal);
        }

        public void setDefaultParallel(int defaultParallel) {
            BuildParams.defaultParallel = defaultParallel;
        }
    }

    /**
     * Indicates that a build parameter is initialized at task execution time and is not available at project configuration time.
     * Attempts to read an uninitialized parameter wil result in an {@link IllegalStateException}.
     */
    @Target({ElementType.METHOD, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface ExecutionTime {}
}
