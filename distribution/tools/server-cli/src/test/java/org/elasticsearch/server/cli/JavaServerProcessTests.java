/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class JavaServerProcessTests extends ESTestCase {

    private static final ExecutorService mockJvmProcessExecutor = Executors.newSingleThreadExecutor();
    Path esConfigDir;

    /*
    @Before
    public void setupDummyInstallation() throws IOException {
        sysprops.put("java.home", "/javahome");
        esConfigDir = esHomeDir.resolve("config");
        Files.createDirectories(esConfigDir);
        Files.writeString(esConfigDir.resolve("jvm.options"), "");
    }

    @AfterClass
    public static void cleanupExecutor() {
        mockJvmProcessExecutor.shutdown();
    }

    @Override
    protected void assertUsage(Matcher<String> matcher, String... args) throws Exception {
        mainCallback = FAIL_MAIN;
        super.assertUsage(matcher, args);
    }

    private void assertMutuallyExclusiveOptions(String... args) throws Exception {
        assertUsage(allOf(containsString("ERROR:"), containsString("are unavailable given other options on the command line")), args);
    */

    /*
      TO TEST
     createProcess failure (UserException, InterruptedException, IOException)
     createProcess
      - env vars passed through
      - temp dir setup
      - ES_JAVA_OPTS passed to getJvmOptions
      - distribution type passed through
      - jvm options returned from getJvmOptions passed through
      - processInfo.workingDir() used as process builder working dir
      - java.home used for java bin
      - java path on windows ends in exe
      - classpath slashes for windows/nix
      - main classname
      - output fd inherited, others piped
     ServerArgs written to stdin
     ServerArgs error writing fallthrough
     stderr written through
     user exception written to stderr
     empty stderr
     stderr ioexception rethrown
     ready signal on stderr
     dead (ready == false) waits for exit and throws userException
     detach waits for stderr done, closes streams

     */

    interface MainMethod {
        void main(ServerArgs args, OutputStream stdout, OutputStream stderr, AtomicInteger exitCode);
    }

    MainMethod mainCallback;
    final MainMethod FAIL_MAIN = (args, stdout, stderr, exitCode) -> fail("Did not expect to run init");

    @Before
    public void resetCommand() {
        mainCallback = null;
    }

    // a "process" that is really another thread
    private class MockElasticsearchProcess extends Process {
        private final PipedOutputStream processStdin = new PipedOutputStream();
        private final PipedInputStream processStdout = new PipedInputStream();
        private final PipedInputStream processStderr = new PipedInputStream();
        private final PipedInputStream stdin = new PipedInputStream();
        private final PipedOutputStream stdout = new PipedOutputStream();
        private final PipedOutputStream stderr = new PipedOutputStream();

        private final AtomicInteger exitCode = new AtomicInteger();
        private final AtomicReference<IOException> argsParsingException = new AtomicReference<>();
        private final Future<?> main;

        MockElasticsearchProcess() throws IOException {
            stdin.connect(processStdin);
            stdout.connect(processStdout);
            stderr.connect(processStderr);
            this.main = mockJvmProcessExecutor.submit(() -> {
                try (var in = new InputStreamStreamInput(stdin)) {
                    final ServerArgs serverArgs = new ServerArgs(in);
                    if (mainCallback != null) {
                        mainCallback.main(serverArgs, stdout, stderr, exitCode);
                    }
                } catch (IOException e) {
                    argsParsingException.set(e);
                }
                IOUtils.closeWhileHandlingException(stdin, stdout, stderr);
            });
        }

        @Override
        public OutputStream getOutputStream() {
            return processStdin;
        }

        @Override
        public InputStream getInputStream() {
            return processStdout;
        }

        @Override
        public InputStream getErrorStream() {
            return processStderr;
        }

        @Override
        public long pid() {
            return 12345;
        }

        @Override
        public int waitFor() throws InterruptedException {
            try {
                main.get();
            } catch (ExecutionException e) {
                throw new AssertionError(e);
            }
            if (argsParsingException.get() != null) {
                throw new AssertionError("Reading server args failed", argsParsingException.get());
            }
            return exitCode.get();
        }

        @Override
        public int exitValue() {
            if (main.isDone() == false) {
                throw new IllegalThreadStateException(); // match spec
            }
            return exitCode.get();
        }

        @Override
        public void destroy() {
            fail("Tried to kill ES process");
        }
    }

    /*
    @Override
    protected Command newCommand() {
        return new ServerCli() {

            @Override
            protected List<String> getJvmOptions(Path configDir, Path pluginsDir, Path tmpDir, String envOptions) throws Exception {
                return new ArrayList<>();
            }

            @Override
            protected Process startProcess(ProcessBuilder processBuilder) throws IOException {
                // TODO: validate processbuilder stuff
                return new MockElasticsearchProcess();
            }

            @Override
            protected Command loadTool(String toolname, String libs) {
                assertThat(toolname, equalTo("auto-configure-node"));
                assertThat(libs, equalTo("modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli"));
                return AUTO_CONFIG_CLI;
            }
        };
    }*/

}
