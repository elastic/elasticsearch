package org.elasticsearch.gradle;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Internal;
import org.gradle.process.BaseExecSpec;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;
import org.gradle.process.JavaExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
@SuppressWarnings("unchecked")
public class LoggedExec extends Exec {

    private Consumer<Logger> outputLogger;
    
    public LoggedExec() {

        if (getLogger().isInfoEnabled() == false) {
            setIgnoreExitValue(true);
            setSpoolOutput(false);
            // We use an anonymous inner class here because Gradle cannot properly snapshot this input for the purposes of
            // incremental build if we use a lambda. This ensures LoggedExec tasks that declare output can be UP-TO-DATE.
            doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    if (LoggedExec.this.getExecResult().getExitValue() != 0) {
                        try {
                            LoggedExec.this.getLogger().error("Output for " + LoggedExec.this.getExecutable() + ":");
                            outputLogger.accept(LoggedExec.this.getLogger());
                        } catch (Exception e) {
                            throw new GradleException("Failed to read exec output", e);
                        }
                        throw new GradleException(
                            String.format(
                                "Process '%s %s' finished with non-zero exit value %d",
                                LoggedExec.this.getExecutable(),
                                LoggedExec.this.getArgs(),
                                LoggedExec.this.getExecResult().getExitValue()
                            )
                        );
                    }
                }
            });
        }
    }

    @Internal
    public void setSpoolOutput(boolean spoolOutput) {
        final OutputStream out;
        if (spoolOutput) {
            File spoolFile = new File(getProject().getBuildDir() + "/buffered-output/" + this.getName());
            out = new LazyFileOutputStream(spoolFile);
            outputLogger = logger -> {
                try {
                    // the file may not exist if the command never output anything
                    if (Files.exists(spoolFile.toPath())) {
                        Files.lines(spoolFile.toPath()).forEach(logger::error);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("could not log", e);
                }
            };
        } else {
            out = new ByteArrayOutputStream();
            outputLogger = logger -> {
                try {
                    logger.error(((ByteArrayOutputStream) out).toString("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        setStandardOutput(out);
        setErrorOutput(out);
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
