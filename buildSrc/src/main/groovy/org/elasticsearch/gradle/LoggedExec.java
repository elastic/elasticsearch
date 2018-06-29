package org.elasticsearch.gradle;

import groovy.lang.Closure;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.tasks.Exec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
public class LoggedExec extends Exec {

    protected ByteArrayOutputStream output = new ByteArrayOutputStream();

    public LoggedExec() {
        if (getLogger().isInfoEnabled() == false) {
            setStandardOutput(output);
            setErrorOutput(output);
            setIgnoreExitValue(true);
            doLast(new Closure<Void>(this, this) {
                public void doCall(Task it) throws IOException {
                    if (getExecResult().getExitValue() != 0) {
                        for (String line : output.toString("UTF-8").split("\\R")) {
                            getLogger().error(line);
                        }
                        throw new GradleException(
                                "Process \'" + getExecutable() + " " +
                                        getArgs().stream().collect(Collectors.joining(" "))+
                                        "\' finished with non-zero exit value " +
                                        String.valueOf(getExecResult().getExitValue())
                        );
                    }
                }
            });
        }
    }
}
