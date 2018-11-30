/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.monitor.jvm.JvmInfo;

import com.sun.jna.IntegerType;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;
import com.sun.jna.ptr.IntByReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;


/**
 * Covers positive test cases for create named pipes, which are not possible in Java with
 * the Elasticsearch security manager configuration or seccomp.  This is why the class extends
 * LuceneTestCase rather than ESTestCase.
 *
 * The way that pipes are managed in this class, e.g. using the mkfifo shell command, is
 * not suitable for production, but adequate for this test.
 */
public class NamedPipeHelperNoBootstrapTests extends LuceneTestCase {

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private static final String HELLO_WORLD = "Hello, world!";
    private static final String GOODBYE_WORLD = "Goodbye, world!";

    private static final int BUFFER_SIZE = 4096;

    private static final long PIPE_ACCESS_OUTBOUND = 2;
    private static final long PIPE_ACCESS_INBOUND = 1;
    private static final long PIPE_TYPE_BYTE = 0;
    private static final long PIPE_WAIT = 0;
    private static final long PIPE_REJECT_REMOTE_CLIENTS = 8;
    private static final long NMPWAIT_USE_DEFAULT_WAIT = 0;

    private static final int ERROR_PIPE_CONNECTED = 535;

    private static final Pointer INVALID_HANDLE_VALUE = Pointer.createConstant(Pointer.SIZE == 8 ? -1 : 0xFFFFFFFFL);

    static {
        // Have to use JNA for Windows named pipes
        if (Constants.WINDOWS) {
            Native.register("kernel32");
        }
    }

    public static class DWord extends IntegerType {

        public DWord() {
            super(4, 0, true);
        }

        public DWord(long val) {
            super(4, val, true);
        }
    }

    // https://msdn.microsoft.com/en-us/library/windows/desktop/aa365150(v=vs.85).aspx
    private static native Pointer CreateNamedPipeW(WString name, DWord openMode, DWord pipeMode, DWord maxInstances, DWord outBufferSize,
            DWord inBufferSize, DWord defaultTimeOut, Pointer securityAttributes);

    // https://msdn.microsoft.com/en-us/library/windows/desktop/aa365146(v=vs.85).aspx
    private static native boolean ConnectNamedPipe(Pointer handle, Pointer overlapped);

    // https://msdn.microsoft.com/en-us/library/windows/desktop/ms724211(v=vs.85).aspx
    private static native boolean CloseHandle(Pointer handle);

    // https://msdn.microsoft.com/en-us/library/windows/desktop/aa365467(v=vs.85).aspx
    private static native boolean ReadFile(Pointer handle, Pointer buffer, DWord numberOfBytesToRead, IntByReference numberOfBytesRead,
            Pointer overlapped);

    // https://msdn.microsoft.com/en-us/library/windows/desktop/aa365747(v=vs.85).aspx
    private static native boolean WriteFile(Pointer handle, Pointer buffer, DWord numberOfBytesToWrite, IntByReference numberOfBytesWritten,
            Pointer overlapped);

    private static Pointer createPipe(String pipeName, boolean forWrite) throws IOException, InterruptedException {
        if (Constants.WINDOWS) {
            return createPipeWindows(pipeName, forWrite);
        }
        createPipeUnix(pipeName);
        // This won't be used in the *nix version
        return INVALID_HANDLE_VALUE;
    }

    private static void createPipeUnix(String pipeName) throws IOException, InterruptedException {
        if (Runtime.getRuntime().exec("mkfifo " + pipeName).waitFor() != 0) {
            throw new IOException("mkfifo failed for pipe " + pipeName);
        }
    }

    private static Pointer createPipeWindows(String pipeName, boolean forWrite) throws IOException {
        Pointer handle = CreateNamedPipeW(new WString(pipeName), new DWord(forWrite ? PIPE_ACCESS_OUTBOUND : PIPE_ACCESS_INBOUND),
                new DWord(PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS), new DWord(1),
                new DWord(BUFFER_SIZE), new DWord(BUFFER_SIZE), new DWord(NMPWAIT_USE_DEFAULT_WAIT), Pointer.NULL);
        if (INVALID_HANDLE_VALUE.equals(handle)) {
            throw new IOException("CreateNamedPipeW failed for pipe " + pipeName + " with error " + Native.getLastError());
        }
        return handle;
    }

    private static String readLineFromPipe(String pipeName, Pointer handle) throws IOException {
        if (Constants.WINDOWS) {
            return readLineFromPipeWindows(pipeName, handle);
        }
        return readLineFromPipeUnix(pipeName);
    }

    private static String readLineFromPipeUnix(String pipeName) throws IOException {
        return Files.readAllLines(PathUtils.get(pipeName), StandardCharsets.UTF_8).get(0);
    }

    private static String readLineFromPipeWindows(String pipeName, Pointer handle) throws IOException {
        if (!ConnectNamedPipe(handle, Pointer.NULL)) {
            // ERROR_PIPE_CONNECTED means the pipe was already connected so
            // there was no need to connect it again - not a problem
            if (Native.getLastError() != ERROR_PIPE_CONNECTED) {
                throw new IOException("ConnectNamedPipe failed for pipe " + pipeName + " with error " + Native.getLastError());
            }
        }
        IntByReference numberOfBytesRead = new IntByReference();
        ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        if (!ReadFile(handle, Native.getDirectBufferPointer(buf), new DWord(BUFFER_SIZE), numberOfBytesRead, Pointer.NULL)) {
            throw new IOException("ReadFile failed for pipe " + pipeName + " with error " + Native.getLastError());
        }
        byte[] content = new byte[numberOfBytesRead.getValue()];
        buf.get(content);
        String line = new String(content, StandardCharsets.UTF_8);
        int newlinePos = line.indexOf('\n');
        if (newlinePos == -1) {
            return line;
        }
        return line.substring(0, newlinePos);
    }

    private static void writeLineToPipe(String pipeName, Pointer handle, String line) throws IOException {
        if (Constants.WINDOWS) {
            writeLineToPipeWindows(pipeName, handle, line);
        } else {
            writeLineToPipeUnix(pipeName, line);
        }
    }

    private static void writeLineToPipeUnix(String pipeName, String line) throws IOException {
        Files.write(PathUtils.get(pipeName), (line + '\n').getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE);
    }

    private static void writeLineToPipeWindows(String pipeName, Pointer handle, String line) throws IOException {
        if (!ConnectNamedPipe(handle, Pointer.NULL)) {
            // ERROR_PIPE_CONNECTED means the pipe was already connected so
            // there was no need to connect it again - not a problem
            if (Native.getLastError() != ERROR_PIPE_CONNECTED) {
                throw new IOException("ConnectNamedPipe failed for pipe " + pipeName + " with error " + Native.getLastError());
            }
        }
        IntByReference numberOfBytesWritten = new IntByReference();
        ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        buf.put((line + '\n').getBytes(StandardCharsets.UTF_8));
        if (!WriteFile(handle, Native.getDirectBufferPointer(buf), new DWord(buf.position()), numberOfBytesWritten, Pointer.NULL)) {
            throw new IOException("WriteFile failed for pipe " + pipeName + " with error " + Native.getLastError());
        }
    }

    private static void deletePipe(String pipeName, Pointer handle) throws IOException {
        if (Constants.WINDOWS) {
            deletePipeWindows(pipeName, handle);
        } else {
            deletePipeUnix(pipeName);
        }
    }

    private static void deletePipeUnix(String pipeName) throws IOException {
        Files.delete(PathUtils.get(pipeName));
    }

    private static void deletePipeWindows(String pipeName, Pointer handle) throws IOException {
        if (!CloseHandle(handle)) {
            throw new IOException("CloseHandle failed for pipe " + pipeName + " with error " + Native.getLastError());
        }
    }

    private static class PipeReaderServer extends Thread {

        private String pipeName;
        private String line;
        private Exception exception;

        PipeReaderServer(String pipeName) {
            this.pipeName = pipeName;
        }

        @Override
        public void run() {
            Pointer handle = INVALID_HANDLE_VALUE;
            try {
                handle = createPipe(pipeName, false);
                line = readLineFromPipe(pipeName, handle);
            }
            catch (IOException | InterruptedException e) {
                exception = e;
            }
            try {
                deletePipe(pipeName, handle);
            } catch (IOException e) {
                // Ignore it if the previous block caught an exception, as this probably means we failed to create the pipe
                if (exception == null) {
                    exception = e;
                }
            }
        }

        public String getLine() {
            return line;
        }

        public Exception getException() {
            return exception;
        }
    }

    private static class PipeWriterServer extends Thread {

        private String pipeName;
        private String line;
        private Exception exception;

        PipeWriterServer(String pipeName, String line) {
            this.pipeName = pipeName;
            this.line = line;
        }

        @Override
        public void run() {
            Pointer handle = INVALID_HANDLE_VALUE;
            try {
                handle = createPipe(pipeName, true);
                writeLineToPipe(pipeName, handle, line);
            } catch (IOException | InterruptedException e) {
                exception = e;
            }
            try {
                deletePipe(pipeName, handle);
            } catch (IOException e) {
                // Ignore it if the previous block caught an exception, as this probably means we failed to create the pipe
                if (exception == null) {
                    exception = e;
                }
            }
        }

        public Exception getException() {
            return exception;
        }
    }

    public void testOpenForInput() throws IOException, InterruptedException {
        Environment env = TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build());
        String pipeName = NAMED_PIPE_HELPER.getDefaultPipeDirectoryPrefix(env) + "inputPipe" + JvmInfo.jvmInfo().pid();

        PipeWriterServer server = new PipeWriterServer(pipeName, HELLO_WORLD);
        server.start();
        try {
            // Timeout is 10 seconds for the very rare case of Amazon EBS volumes created from snapshots
            // being slow the first time a particular disk block is accessed.  The same problem as
            // https://github.com/elastic/x-pack-elasticsearch/issues/922, which was fixed by
            // https://github.com/elastic/x-pack-elasticsearch/pull/987, has been observed in CI tests.
            InputStream is = NAMED_PIPE_HELPER.openNamedPipeInputStream(pipeName, Duration.ofSeconds(10));
            assertNotNull(is);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                assertEquals(HELLO_WORLD, line);
            }
        } catch (IOException e) {
            server.interrupt();
            throw e;
        } finally {
            // If this doesn't join quickly then the server thread is probably deadlocked so there's no
            // point waiting a long time.
            server.join(1000);
        }

        assertNull(server.getException());
    }

    public void testOpenForOutput() throws IOException, InterruptedException {
        Environment env = TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build());
        String pipeName = NAMED_PIPE_HELPER.getDefaultPipeDirectoryPrefix(env) + "outputPipe" + JvmInfo.jvmInfo().pid();

        PipeReaderServer server = new PipeReaderServer(pipeName);
        server.start();
        try {
            // Timeout is 10 seconds for the very rare case of Amazon EBS volumes created from snapshots
            // being slow the first time a particular disk block is accessed.  The same problem as
            // https://github.com/elastic/x-pack-elasticsearch/issues/922, which was fixed by
            // https://github.com/elastic/x-pack-elasticsearch/pull/987, has been observed in CI tests.
            OutputStream os = NAMED_PIPE_HELPER.openNamedPipeOutputStream(pipeName, Duration.ofSeconds(10));
            assertNotNull(os);

            // In some rare cases writer can close before the reader has had a chance
            // to read what is written. On Windows this can cause ConnectNamedPipe to
            // error with ERROR_NO_DATA
            try (OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
                writer.write(GOODBYE_WORLD);
                writer.write('\n');
            }
        } catch (IOException e) {
            server.interrupt();
            throw e;
        } finally {
            // If this doesn't join quickly then the server thread is probably deadlocked so there's no
            // point waiting a long time.
            server.join(1000);
        }

        assertNull(server.getException());
        assertEquals(GOODBYE_WORLD, server.getLine());
    }
}
