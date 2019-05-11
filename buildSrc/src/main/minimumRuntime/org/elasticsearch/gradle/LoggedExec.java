package org.elasticsearch.gradle;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Exec;
import org.gradle.process.BaseExecSpec;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;
import org.gradle.process.JavaExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
@SuppressWarnings("unchecked")
public class LoggedExec extends Exec {

    // helper to allow lambda creation using Files methods
    private interface ThrowingStringSupplier {
        String get() throws Exception;
    }

    private Property<Boolean> useFileBuffer;
    
    public LoggedExec() {
        this.useFileBuffer = getProject().getObjects().property(Boolean.class);
        this.useFileBuffer.set(false); // default to in memory
        if (getLogger().isInfoEnabled() == false) {
            getProject().afterEvaluate(p -> {
                OutputStream stdout;
                OutputStream stderr;
                ThrowingStringSupplier readStdout;
                ThrowingStringSupplier readStderr;

                try {
                    if (useFileBuffer.get()) {
                        String outputdir = p.getBuildDir() + "/buffered-output/" + this.getName();
                        p.mkdir(outputdir);
                        stdout = new FileOutputStream(outputdir + "/stdout");
                        stderr = new FileOutputStream(outputdir + "/stderr");
                        readStdout = () -> Files.readString(Paths.get(outputdir + "/stdout"));
                        readStderr = () -> Files.readString(Paths.get(outputdir + "/stderr"));
                    } else {
                        stdout = new ByteArrayOutputStream();
                        stderr = new ByteArrayOutputStream();
                        readStdout = () -> ((ByteArrayOutputStream) stdout).toString(StandardCharsets.UTF_8);
                        readStderr = () -> ((ByteArrayOutputStream) stderr).toString(StandardCharsets.UTF_8);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                setStandardOutput(stdout);
                setErrorOutput(stderr);
                setIgnoreExitValue(true);
                doLast(task -> {
                    if (getExecResult().getExitValue() != 0) {
                        try {
                            getLogger().error("Standard output:");
                            getLogger().error(readStdout.get());
                            getLogger().error("Standard error:");
                            getLogger().error(readStderr.get());
                        } catch (Exception e) {
                            throw new GradleException("Failed to read exec output", e);
                        }
                        throw new GradleException(
                            String.format(
                                "Process '%s %s' finished with non-zero exit value %d",
                                getExecutable(),
                                getArgs(),
                                getExecResult().getExitValue()
                            )
                        );
                    }
                });
            });
        }
    }

    public void setUseFileBuffer(boolean useFileBuffer) {
        this.useFileBuffer.set(useFileBuffer);
    }

    public static ExecResult exec(Project project, Action<ExecSpec> action) {
        return genericExec(project, project::exec, action);
    }

    public static ExecResult javaexec(Project project, Action<JavaExecSpec> action) {
        return genericExec(project, project::javaexec, action);
    }

    private static <T extends BaseExecSpec>  ExecResult genericExec(
        Project project,
        Function<Action<T>,ExecResult> function,
        Action<T> action
    ) {
        if (project.getLogger().isInfoEnabled()) {
            return function.apply(action);
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        try {
            return function.apply(spec -> {
                spec.setStandardOutput(output);
                spec.setErrorOutput(error);
                action.execute(spec);
            });
        } catch (Exception e) {
            try {
                project.getLogger().error("Standard output:");
                project.getLogger().error(output.toString("UTF-8"));
                project.getLogger().error("Standard error:");
                project.getLogger().error(error.toString("UTF-8"));
            } catch (UnsupportedEncodingException ue) {
                throw new GradleException("Failed to read exec output", ue);
            }
            throw e;
        }
    }
}
