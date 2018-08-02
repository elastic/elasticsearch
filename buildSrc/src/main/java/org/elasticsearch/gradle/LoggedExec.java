package org.elasticsearch.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.tasks.Exec;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
@SuppressWarnings("unchecked")
public class LoggedExec extends Exec {

    protected ByteArrayOutputStream output = new ByteArrayOutputStream();

    public LoggedExec() {
        if (getLogger().isInfoEnabled() == false) {
            setStandardOutput(output);
            setErrorOutput(output);
            setIgnoreExitValue(true);
            doLast((unused) -> {
                    if (getExecResult().getExitValue() != 0) {
                        try {
                            for (String line : output.toString("UTF-8").split("\\R")) {
                                getLogger().error(line);
                            }
                        } catch (UnsupportedEncodingException e) {
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
                }
            );
        }
    }
}
