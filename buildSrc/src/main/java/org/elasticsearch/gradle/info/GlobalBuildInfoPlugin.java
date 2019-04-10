package org.elasticsearch.gradle.info;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.internal.jvm.Jvm;
import org.gradle.process.ExecResult;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"));

        File compilerJavaHome = findCompilerJavaHome();
        File runtimeJavaHome = findRuntimeJavaHome(compilerJavaHome);

        final Map<Integer, String> javaVersions = new HashMap<>();
        for (int version = 8; version <= Integer.parseInt(minimumCompilerVersion.getMajorVersion()); version++) {
            if (System.getenv(getJavaHomeEnvVarName(Integer.toString(version))) != null) {
                javaVersions.put(version, findJavaHome(Integer.toString(version)));
            }
        }

        project.getTasks().register("generateGlobalBuildInfo", GlobalBuildInfoGeneratingTask.class, task -> {
            task.setMinimumCompilerVersion(minimumCompilerVersion);
            task.setMinimumRuntimeVersion(minimumRuntimeVersion);
            task.setCompilerJavaHome(compilerJavaHome);
            task.setRuntimeJavaHome(runtimeJavaHome);
            task.setOutputFile(new File(project.getBuildDir(), "global-build-info"));
        });

        String javaVendor = System.getProperty("java.vendor");
        String gradleJavaVersion = System.getProperty("java.version");
        String gradleJavaVersionDetails = javaVendor + " " + gradleJavaVersion + " [" + System.getProperty("java.vm.name") + " "  + System.getProperty("java.vm.version") + "]";

        String compilerJavaVersionDetails = gradleJavaVersionDetails;
        JavaVersion compilerJavaVersionEnum = JavaVersion.current();
        String runtimeJavaVersionDetails = gradleJavaVersionDetails;
        JavaVersion runtimeJavaVersionEnum = JavaVersion.current();

        try {
            if (Files.isSameFile(compilerJavaHome.toPath(), gradleJavaHome.toPath()) == false) {
                compilerJavaVersionDetails = findJavaVersionDetails(project, compilerJavaHome);
                compilerJavaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(project, compilerJavaHome));
            }

            if (Files.isSameFile(runtimeJavaHome.toPath(), gradleJavaHome.toPath()) == false) {
                runtimeJavaVersionDetails = findJavaVersionDetails(project, runtimeJavaHome);
                runtimeJavaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(project, runtimeJavaHome));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String inFipsJvmScript = "print(java.security.Security.getProviders()[0].name.toLowerCase().contains(\"fips\"));";
        boolean inFipsJvm = Boolean.parseBoolean(runJavaAsScript(project, runtimeJavaHome, inFipsJvmScript));

        // Build debugging info
        LOGGER.lifecycle("=======================================");
        LOGGER.lifecycle("Elasticsearch Build Hamster says Hello!");
        LOGGER.lifecycle("  Gradle Version        : " + project.getGradle().getGradleVersion());
        LOGGER.lifecycle("  OS Info               : " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " (" + System.getProperty("os.arch") + ")");
        if (gradleJavaVersionDetails.equals(compilerJavaVersionDetails) == false || gradleJavaVersionDetails.equals(runtimeJavaVersionDetails) == false) {
            LOGGER.lifecycle("  Compiler JDK Version  : " + compilerJavaVersionEnum + " (" + compilerJavaVersionDetails + ")");
            LOGGER.lifecycle("  Compiler java.home    : " + compilerJavaHome);
            LOGGER.lifecycle("  Runtime JDK Version   : " + runtimeJavaVersionEnum + " (" +runtimeJavaVersionDetails + ")");
            LOGGER.lifecycle("  Runtime java.home     : " + runtimeJavaHome);
            LOGGER.lifecycle("  Gradle JDK Version    : " + JavaVersion.toVersion(gradleJavaVersion) + " (" + gradleJavaVersionDetails + ")");
            LOGGER.lifecycle("  Gradle java.home      : " + gradleJavaHome);
        } else {
            LOGGER.lifecycle("  JDK Version           : " + JavaVersion.toVersion(gradleJavaVersion) + " (" + gradleJavaVersionDetails + ")");
            LOGGER.lifecycle("  JAVA_HOME             : " + gradleJavaHome);
        }
        //LOGGER.lifecycle("  Random Testing Seed   : " + project.property("testSeed"));
        LOGGER.lifecycle("=======================================");

        // enforce Java version
        if (compilerJavaVersionEnum.compareTo(minimumCompilerVersion) < 0) {
            final String message =
                "the compiler java.home must be set to a JDK installation directory for Java ${minimumCompilerVersion}" +
                    " but is [${compilerJavaHome}] corresponding to [${compilerJavaVersionEnum}]";
            throw new GradleException(message);
        }

        if (runtimeJavaVersionEnum.compareTo(minimumRuntimeVersion) < 0) {
            final String message =
                "the runtime java.home must be set to a JDK installation directory for Java ${minimumRuntimeVersion}" +
                    " but is [${runtimeJavaHome}] corresponding to [${runtimeJavaVersionEnum}]";
            throw new GradleException(message);
        }

        for (final Map.Entry<Integer, String> javaVersionEntry : javaVersions.entrySet()) {
            final String javaHome = javaVersionEntry.getValue();
            if (javaHome == null) {
                continue;
            }
            JavaVersion javaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(project, new File(javaHome)));
            final JavaVersion expectedJavaVersionEnum;
            final int version = javaVersionEntry.getKey();
            if (version < 9) {
                expectedJavaVersionEnum = JavaVersion.toVersion("1." + version);
            } else {
                expectedJavaVersionEnum = JavaVersion.toVersion(Integer.toString(version));
            }
            if (javaVersionEnum != expectedJavaVersionEnum) {
                final String message =
                    "the environment variable JAVA" + version + "_HOME must be set to a JDK installation directory for Java" +
                        " ${expectedJavaVersionEnum} but is [${javaHome}] corresponding to [${javaVersionEnum}]";
                throw new GradleException(message);
            }
        }

        JavaVersion finalCompilerJavaVersionEnum = compilerJavaVersionEnum;
        JavaVersion finalRuntimeJavaVersionEnum = runtimeJavaVersionEnum;
        project.allprojects(p -> {
            ExtraPropertiesExtension ext = p.getExtensions().getByType(ExtraPropertiesExtension.class);

            ext.set("compilerJavaHome", compilerJavaHome);
            ext.set("runtimeJavaHome", runtimeJavaHome);
            ext.set("compilerJavaVersion", finalCompilerJavaVersionEnum);
            ext.set("runtimeJavaVersion", finalRuntimeJavaVersionEnum);
            ext.set("isRuntimeJavaHomeSet", compilerJavaHome.equals(runtimeJavaHome) == false);
            ext.set("javaVersions", javaVersions);
            ext.set("minimumCompilerVersion", minimumCompilerVersion);
            ext.set("minimumRuntimeVersion", minimumRuntimeVersion);
            ext.set("inFipsJvm", inFipsJvm);
            ext.set("gradleJavaVersion", JavaVersion.toVersion(gradleJavaVersion));
        });
    }

    private static File findCompilerJavaHome() {
        String compilerJavaHome = System.getenv("JAVA_HOME");
        String compilerJavaProperty = System.getProperty("compiler.java");

        if (compilerJavaProperty != null) {
            compilerJavaHome = findJavaHome(compilerJavaProperty);
        }

        // if JAVA_HOME is not set,so we use the JDK that Gradle was run with.
        return Objects.requireNonNullElseGet(new File(compilerJavaHome), () -> Jvm.current().getJavaHome());
    }

    private static File findRuntimeJavaHome(final File compilerJavaHome) {
        String runtimeJavaProperty = System.getProperty("runtime.java");

        if (runtimeJavaProperty != null) {
            return new File(findJavaHome(runtimeJavaProperty));
        }

        return Objects.requireNonNullElse(new File(System.getenv("RUNTIME_JAVA_HOME")), compilerJavaHome);
    }

    private static String findJavaHome(String version) {
        String versionedVarName = getJavaHomeEnvVarName(version);
        String versionedJavaHome = System.getenv(versionedVarName);
        if (versionedJavaHome == null) {
            throw new GradleException(
                "$versionedVarName must be set to build Elasticsearch. " +
                    "Note that if the variable was just set you might have to run `./gradlew --stop` for " +
                    "it to be picked up. See https://github.com/elastic/elasticsearch/issues/31399 details."
            );
        }
        return versionedJavaHome;
    }

    private static String getJavaHomeEnvVarName(String version) {
        return "JAVA" + version + "_HOME";
    }

    private static String getResourceContents(String resourcePath) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(GlobalBuildInfoPlugin.class.getResourceAsStream(resourcePath)))) {
            StringBuffer buffer = new StringBuffer();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (buffer.length() != 0) {
                    buffer.append('\n');
                }
                buffer.append(line);
            }

            return buffer.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Error trying to read classpath resource: " + resourcePath, e);
        }
    }

    /** Finds printable java version of the given JAVA_HOME */
    private static String findJavaVersionDetails(Project project, File javaHome) {
        String versionInfoScript = "print(" +
            "java.lang.System.getProperty(\"java.vendor\") + \" \" + java.lang.System.getProperty(\"java.version\") + " +
            "\" [\" + java.lang.System.getProperty(\"java.vm.name\") + \" \" + java.lang.System.getProperty(\"java.vm.version\") + \"]\");";
        return runJavaAsScript(project, javaHome, versionInfoScript).trim();
    }

    /** Finds the parsable java specification version */
    private static String findJavaSpecificationVersion(Project project, File javaHome) {
        String versionScript = "print(java.lang.System.getProperty(\"java.specification.version\"));";
        return runJavaAsScript(project, javaHome, versionScript);
    }

    /** Runs the given javascript using jjs from the jdk, and returns the output */
    private static String runJavaAsScript(Project project, File javaHome, String script) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // gradle/groovy does not properly escape the double quote for windows
            script = script.replace("\"", "\\\"");
        }
        File jrunscriptPath = new File(javaHome, "bin/jrunscript");
        String finalScript = script;
        ExecResult result = project.exec(spec -> {
            spec.setExecutable(jrunscriptPath);
            spec.args("-e", finalScript);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stderr);
            spec.setIgnoreExitValue(true);
        });

        try {
            if (result.getExitValue() != 0) {
                LOGGER.error("STDOUT:");
                stdout.toString("UTF-8").lines().forEach(LOGGER::error);
                LOGGER.error("STDERR:");
                stderr.toString("UTF-8").lines().forEach(LOGGER::error);
                result.rethrowFailure();
            }
            return stdout.toString("UTF-8").trim();
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
