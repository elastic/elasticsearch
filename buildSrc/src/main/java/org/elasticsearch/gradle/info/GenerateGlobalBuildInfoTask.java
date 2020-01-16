package org.elasticsearch.gradle.info;

import org.elasticsearch.gradle.OS;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.jvm.Jvm;
import org.gradle.process.ExecResult;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;

@CacheableTask
public class GenerateGlobalBuildInfoTask extends DefaultTask {
    private JavaVersion minimumCompilerVersion;
    private JavaVersion minimumRuntimeVersion;
    private File compilerJavaHome;
    private File runtimeJavaHome;
    private List<JavaHome> javaVersions;
    private final RegularFileProperty outputFile;
    private final RegularFileProperty compilerVersionFile;
    private final RegularFileProperty runtimeVersionFile;

    @Inject
    public GenerateGlobalBuildInfoTask(ObjectFactory objectFactory) {
        this.outputFile = objectFactory.fileProperty();
        this.compilerVersionFile = objectFactory.fileProperty();
        this.runtimeVersionFile = objectFactory.fileProperty();
    }

    @Input
    public JavaVersion getMinimumCompilerVersion() {
        return minimumCompilerVersion;
    }

    public void setMinimumCompilerVersion(JavaVersion minimumCompilerVersion) {
        this.minimumCompilerVersion = minimumCompilerVersion;
    }

    @Input
    public JavaVersion getMinimumRuntimeVersion() {
        return minimumRuntimeVersion;
    }

    public void setMinimumRuntimeVersion(JavaVersion minimumRuntimeVersion) {
        this.minimumRuntimeVersion = minimumRuntimeVersion;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getCompilerJavaHome() {
        return compilerJavaHome;
    }

    public void setCompilerJavaHome(File compilerJavaHome) {
        this.compilerJavaHome = compilerJavaHome;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getRuntimeJavaHome() {
        return runtimeJavaHome;
    }

    public void setRuntimeJavaHome(File runtimeJavaHome) {
        this.runtimeJavaHome = runtimeJavaHome;
    }

    @Nested
    public List<JavaHome> getJavaVersions() {
        return javaVersions;
    }

    public void setJavaVersions(List<JavaHome> javaVersions) {
        this.javaVersions = javaVersions;
    }

    @OutputFile
    public RegularFileProperty getOutputFile() {
        return outputFile;
    }

    @OutputFile
    public RegularFileProperty getCompilerVersionFile() {
        return compilerVersionFile;
    }

    @OutputFile
    public RegularFileProperty getRuntimeVersionFile() {
        return runtimeVersionFile;
    }

    @TaskAction
    public void generate() {
        String javaVendorVersion = System.getProperty("java.vendor.version", System.getProperty("java.vendor"));
        String gradleJavaVersion = System.getProperty("java.version");
        String gradleJavaVersionDetails = javaVendorVersion
            + " "
            + gradleJavaVersion
            + " ["
            + System.getProperty("java.vm.name")
            + " "
            + System.getProperty("java.vm.version")
            + "]";

        String compilerJavaVersionDetails = gradleJavaVersionDetails;
        JavaVersion compilerJavaVersionEnum = JavaVersion.current();
        String runtimeJavaVersionDetails = gradleJavaVersionDetails;
        JavaVersion runtimeJavaVersionEnum = JavaVersion.current();
        File gradleJavaHome = Jvm.current().getJavaHome();

        try {
            if (Files.isSameFile(compilerJavaHome.toPath(), gradleJavaHome.toPath()) == false) {
                if (compilerJavaHome.exists()) {
                    compilerJavaVersionDetails = findJavaVersionDetails(compilerJavaHome);
                    compilerJavaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(compilerJavaHome));
                } else {
                    throw new RuntimeException("Compiler Java home path of '" + compilerJavaHome + "' does not exist");
                }
            }

            if (Files.isSameFile(runtimeJavaHome.toPath(), gradleJavaHome.toPath()) == false) {
                if (runtimeJavaHome.exists()) {
                    runtimeJavaVersionDetails = findJavaVersionDetails(runtimeJavaHome);
                    runtimeJavaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(runtimeJavaHome));
                } else {
                    throw new RuntimeException("Runtime Java home path of '" + compilerJavaHome + "' does not exist");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.getAsFile().get()))) {
            final String osName = System.getProperty("os.name");
            final String osVersion = System.getProperty("os.version");
            final String osArch = System.getProperty("os.arch");
            final JavaVersion parsedVersion = JavaVersion.toVersion(gradleJavaVersion);

            writer.write("  Gradle Version        : " + getProject().getGradle().getGradleVersion() + "\n");
            writer.write("  OS Info               : " + osName + " " + osVersion + " (" + osArch + ")\n");

            if (gradleJavaVersionDetails.equals(compilerJavaVersionDetails) == false
                || gradleJavaVersionDetails.equals(runtimeJavaVersionDetails) == false) {
                writer.write("  Compiler JDK Version  : " + compilerJavaVersionEnum + " (" + compilerJavaVersionDetails + ")\n");
                writer.write("  Compiler java.home    : " + compilerJavaHome + "\n");
                writer.write("  Runtime JDK Version   : " + runtimeJavaVersionEnum + " (" + runtimeJavaVersionDetails + ")\n");
                writer.write("  Runtime java.home     : " + runtimeJavaHome + "\n");
                writer.write("  Gradle JDK Version    : " + parsedVersion + " (" + gradleJavaVersionDetails + ")\n");
                writer.write("  Gradle java.home      : " + gradleJavaHome);
            } else {
                writer.write("  JDK Version           : " + parsedVersion + " (" + gradleJavaVersionDetails + ")\n");
                writer.write("  JAVA_HOME             : " + gradleJavaHome);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // enforce Java version
        if (compilerJavaVersionEnum.compareTo(minimumCompilerVersion) < 0) {
            String message = String.format(
                Locale.ROOT,
                "The compiler java.home must be set to a JDK installation directory for Java %s but is [%s] " + "corresponding to [%s]",
                minimumCompilerVersion,
                compilerJavaHome,
                compilerJavaVersionEnum
            );
            throw new GradleException(message);
        }

        if (runtimeJavaVersionEnum.compareTo(minimumRuntimeVersion) < 0) {
            String message = String.format(
                Locale.ROOT,
                "The runtime java.home must be set to a JDK installation directory for Java %s but is [%s] " + "corresponding to [%s]",
                minimumRuntimeVersion,
                runtimeJavaHome,
                runtimeJavaVersionEnum
            );
            throw new GradleException(message);
        }

        for (JavaHome javaVersion : javaVersions) {
            File javaHome = javaVersion.getJavaHome();
            if (javaHome == null) {
                continue;
            }
            JavaVersion javaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(javaHome));
            JavaVersion expectedJavaVersionEnum;
            int version = javaVersion.getVersion();
            if (version < 9) {
                expectedJavaVersionEnum = JavaVersion.toVersion("1." + version);
            } else {
                expectedJavaVersionEnum = JavaVersion.toVersion(Integer.toString(version));
            }
            if (javaVersionEnum != expectedJavaVersionEnum) {
                String message = String.format(
                    Locale.ROOT,
                    "The environment variable JAVA%d_HOME must be set to a JDK installation directory for Java"
                        + " %s but is [%s] corresponding to [%s]",
                    version,
                    expectedJavaVersionEnum,
                    javaHome,
                    javaVersionEnum
                );
                throw new GradleException(message);
            }
        }

        writeToFile(compilerVersionFile.getAsFile().get(), compilerJavaVersionEnum.name());
        writeToFile(runtimeVersionFile.getAsFile().get(), runtimeJavaVersionEnum.name());
    }

    private void writeToFile(File file, String content) {
        try (Writer writer = new FileWriter(file)) {
            writer.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Finds printable java version of the given JAVA_HOME
     */
    private String findJavaVersionDetails(File javaHome) {
        String versionInfoScript = "print("
            + "java.lang.System.getProperty(\"java.vendor.version\", java.lang.System.getProperty(\"java.vendor\")) + \" \" + "
            + "java.lang.System.getProperty(\"java.version\") + \" [\" + "
            + "java.lang.System.getProperty(\"java.vm.name\") + \" \" + "
            + "java.lang.System.getProperty(\"java.vm.version\") + \"]\");";
        return runJavaAsScript(javaHome, versionInfoScript).trim();
    }

    /**
     * Finds the parsable java specification version
     */
    private String findJavaSpecificationVersion(File javaHome) {
        String versionScript = "print(java.lang.System.getProperty(\"java.specification.version\"));";
        return runJavaAsScript(javaHome, versionScript);
    }

    /**
     * Runs the given javascript using jjs from the jdk, and returns the output
     */
    private String runJavaAsScript(File javaHome, String script) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        if (OS.current() == OS.WINDOWS) {
            // gradle/groovy does not properly escape the double quote for windows
            script = script.replace("\"", "\\\"");
        }
        File jrunscriptPath = new File(javaHome, "bin/jrunscript");
        String finalScript = script;
        ExecResult result = getProject().exec(spec -> {
            spec.setExecutable(jrunscriptPath);
            spec.args("-e", finalScript);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stderr);
            spec.setIgnoreExitValue(true);
        });

        if (result.getExitValue() != 0) {
            getLogger().error("STDOUT:");
            Arrays.stream(stdout.toString(UTF_8).split(System.getProperty("line.separator"))).forEach(getLogger()::error);
            getLogger().error("STDERR:");
            Arrays.stream(stderr.toString(UTF_8).split(System.getProperty("line.separator"))).forEach(getLogger()::error);
            result.rethrowFailure();
        }
        return stdout.toString(UTF_8).trim();
    }

}
