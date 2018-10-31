/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handle a stream of C++ log messages that arrive via a named pipe in JSON format.
 * Retains the last few error messages so that they can be passed back in a REST response
 * if the C++ process dies.
 */
public class CppLogMessageHandler implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(CppLogMessageHandler.class);
    private static final int DEFAULT_READBUF_SIZE = 1024;
    private static final int DEFAULT_ERROR_STORE_SIZE = 5;
    private static final long MAX_MESSAGE_INTERVAL_SECONDS = 10;

    private final String jobId;
    private final InputStream inputStream;
    private final int readBufSize;
    private final int errorStoreSize;
    private final Deque<String> errorStore;
    private final CountDownLatch pidLatch;
    private final CountDownLatch cppCopyrightLatch;
    private final CountDownLatch logStreamClosedLatch;
    private MessageSummary lastMessageSummary = new MessageSummary();
    private volatile boolean seenFatalError;
    private volatile long pid;
    private volatile String cppCopyright;

    /**
     * @param jobId May be null or empty if the logs are from a process not associated with a job.
     * @param inputStream May not be null.
     */
    public CppLogMessageHandler(String jobId, InputStream inputStream) {
        this(inputStream, jobId, DEFAULT_READBUF_SIZE, DEFAULT_ERROR_STORE_SIZE);
    }

    /**
     * For testing - allows meddling with the logger, read buffer size and error store size.
     */
    CppLogMessageHandler(InputStream inputStream, String jobId, int readBufSize, int errorStoreSize) {
        this.jobId = jobId;
        this.inputStream = Objects.requireNonNull(inputStream);
        this.readBufSize = readBufSize;
        this.errorStoreSize = errorStoreSize;
        errorStore = ConcurrentCollections.newDeque();
        pidLatch = new CountDownLatch(1);
        cppCopyrightLatch = new CountDownLatch(1);
        logStreamClosedLatch = new CountDownLatch(1);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    /**
     * Tail the InputStream provided to the constructor, handling each complete log document as it arrives.
     * This method will not return until either end-of-file is detected on the InputStream or the
     * InputStream throws an exception.
     */
    public void tailStream() throws IOException {
        XContent xContent = XContentFactory.xContent(XContentType.JSON);
        BytesReference bytesRef = null;
        try {
            byte[] readBuf = new byte[readBufSize];
            for (int bytesRead = inputStream.read(readBuf); bytesRead != -1; bytesRead = inputStream.read(readBuf)) {
                if (bytesRef == null) {
                    bytesRef = new BytesArray(readBuf, 0, bytesRead);
                } else {
                    bytesRef = new CompositeBytesReference(bytesRef, new BytesArray(readBuf, 0, bytesRead));
                }
                bytesRef = parseMessages(xContent, bytesRef);
                readBuf = new byte[readBufSize];
            }
        } finally {
            logStreamClosedLatch.countDown();

            // check if there is some leftover from log summarization
            if (lastMessageSummary.count > 0) {
                logSummarizedMessage();
            }

            // if the process crashed, a non-delimited JSON string might still be in the pipe
            if (bytesRef != null) {
                parseMessage(xContent, bytesRef);
            }
        }
    }

    public boolean hasLogStreamEnded() {
        return logStreamClosedLatch.getCount() == 0;
    }

    public boolean seenFatalError() {
        return seenFatalError;
    }

    public boolean waitForLogStreamClose(Duration timeout) {
        try {
            return logStreamClosedLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Get the process ID of the C++ process whose log messages are being read.  This will
     * arrive in the first log message logged by the C++ process.  They all log their version
     * number immediately on startup so it should not take long to arrive, but will not be
     * available instantly after the process starts.
     */
    public long getPid(Duration timeout) throws TimeoutException {
        // There's an assumption here that 0 is not a valid PID.  This is certainly true for
        // userland processes.  On Windows the "System Idle Process" has PID 0 and on *nix
        // PID 0 is for "sched", which is part of the kernel.
        if (pid == 0) {
            try {
                pidLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (pid == 0) {
                throw new TimeoutException("Timed out waiting for C++ process PID");
            }
        }
        return pid;
    }

    /**
     * Get the process ID of the C++ process whose log messages are being read.  This will
     * arrive in the first log message logged by the C++ process.  They all log a copyright
     * message immediately on startup so it should not take long to arrive, but will not be
     * available instantly after the process starts.
     */
    public String getCppCopyright(Duration timeout) throws TimeoutException {
        if (cppCopyright == null) {
            try {
                cppCopyrightLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (cppCopyright == null) {
                throw new TimeoutException("Timed out waiting for C++ process copyright");
            }
        }
        return cppCopyright;
    }

    /**
     * Extracts version information from the copyright string which assumes a certain format.
     */
    public Map<String, Object> getNativeCodeInfo(Duration timeout) throws TimeoutException {
        String copyrightMessage = getCppCopyright(timeout);
        Matcher matcher = Pattern.compile("Version (.+) \\(Build ([^)]+)\\) Copyright ").matcher(copyrightMessage);
        if (matcher.find()) {
            Map<String, Object> info = new HashMap<>(2);
            info.put("version", matcher.group(1));
            info.put("build_hash", matcher.group(2));
            return info;
        } else {
            // If this happens it probably means someone has changed the format in lib/ver/CBuildInfo.cc
            // in the ml-cpp repo without changing the pattern above to match
            String msg = "Unexpected native process copyright format: " + copyrightMessage;
            LOGGER.error(msg);
            throw new ElasticsearchException(msg);
        }
    }

    /**
     * Expected to be called very infrequently.
     */
    public String getErrors() {
        String[] errorSnapshot = errorStore.toArray(new String[0]);
        StringBuilder errors = new StringBuilder();
        for (String error : errorSnapshot) {
            errors.append(error).append('\n');
        }
        return errors.toString();
    }

    private BytesReference parseMessages(XContent xContent, BytesReference bytesRef) {
        byte marker = xContent.streamSeparator();
        int from = 0;
        while (true) {
            int nextMarker = findNextMarker(marker, bytesRef, from);
            if (nextMarker == -1) {
                // No more markers in this block
                break;
            }
            // Ignore blank lines
            if (nextMarker > from) {
                parseMessage(xContent, bytesRef.slice(from, nextMarker - from));
            }
            from = nextMarker + 1;
            if (from < bytesRef.length() && bytesRef.get(from) == (byte) 0) {
                // This is to work around the problem of log4cxx on Windows
                // outputting UTF-16 instead of UTF-8.  For full details see
                // https://github.com/elastic/machine-learning-cpp/issues/385
                ++from;
            }
        }
        if (from >= bytesRef.length()) {
            return null;
        }
        return bytesRef.slice(from, bytesRef.length() - from);
    }

    private void parseMessage(XContent xContent, BytesReference bytesRef) {
        try (InputStream stream = bytesRef.streamInput();
             XContentParser parser = xContent
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            CppLogMessage msg = CppLogMessage.PARSER.apply(parser, null);
            Level level = Level.getLevel(msg.getLevel());
            if (level == null) {
                // This isn't expected to ever happen
                level = Level.WARN;
            } else if (level.isMoreSpecificThan(Level.ERROR)) {
                // Keep the last few error messages to report if the process dies
                storeError(msg.getMessage());
                if (level.isMoreSpecificThan(Level.FATAL)) {
                    seenFatalError = true;
                }
            }
            long latestPid = msg.getPid();
            if (pid != latestPid) {
                pid = latestPid;
                pidLatch.countDown();
            }
            String latestMessage = msg.getMessage();
            if (cppCopyright == null && latestMessage.contains("Copyright")) {
                cppCopyright = latestMessage;
                cppCopyrightLatch.countDown();
            }

            // get out of here quickly if level isn't of interest
            if (!LOGGER.isEnabled(level)) {
                return;
            }

            // log message summarization is disabled for debug
            if (!LOGGER.isDebugEnabled()) {
                // log summarization: log 1st message, count all consecutive messages arriving
                // in a certain time window and summarize them as 1 message
                if (msg.isSimilarTo(lastMessageSummary.message)
                        && (lastMessageSummary.timestamp.until(msg.getTimestamp(), ChronoUnit.SECONDS) < MAX_MESSAGE_INTERVAL_SECONDS)) {

                    // this is a repeated message, so do not log it, but count
                    lastMessageSummary.count++;
                    lastMessageSummary.message = msg;
                    return;
                    // not similar, flush last summary if necessary
                } else if (lastMessageSummary.count > 0) {
                    // log last message with summary
                    logSummarizedMessage();
                }

                lastMessageSummary.reset(msg.getTimestamp(), msg, level);
            }
            // TODO: Is there a way to preserve the original timestamp when re-logging?
            if (jobId != null) {
                LOGGER.log(level, "[{}] [{}/{}] [{}@{}] {}", jobId, msg.getLogger(), latestPid, msg.getFile(), msg.getLine(),
                        latestMessage);
            } else {
                LOGGER.log(level, "[{}/{}] [{}@{}] {}", msg.getLogger(), latestPid, msg.getFile(), msg.getLine(), latestMessage);
            }
        } catch (XContentParseException e) {
            String upstreamMessage = "Fatal error: '" + bytesRef.utf8ToString() + "'";
            if (upstreamMessage.contains("bad_alloc")) {
                upstreamMessage += ", process ran out of memory";
            }

            // add version information, so it's conveniently next to the crash log
            upstreamMessage += ", version: ";
            try {
                Map<String, Object> versionInfo = getNativeCodeInfo(Duration.ofMillis(10));
                upstreamMessage += String.format(Locale.ROOT, "%s (build %s)", versionInfo.get("version"), versionInfo.get("build_hash"));
            } catch (TimeoutException timeoutException) {
                upstreamMessage += "failed to retrieve";
            }

            storeError(upstreamMessage);
            seenFatalError = true;
        } catch (IOException e) {
            if (jobId != null) {
                LOGGER.warn(new ParameterizedMessage("[{}] IO failure receiving C++ log message: {}",
                        new Object[] {jobId, bytesRef.utf8ToString()}), e);
            } else {
                LOGGER.warn(new ParameterizedMessage("IO failure receiving C++ log message: {}",
                        new Object[] {bytesRef.utf8ToString()}), e);
            }
        }
    }

    private void logSummarizedMessage() {
        // edge case: for 1 repeat, only log the message as is
        if (lastMessageSummary.count > 1) {
            if (jobId != null) {
                LOGGER.log(lastMessageSummary.level, "[{}] [{}/{}] [{}@{}] {} | repeated [{}]", jobId,
                        lastMessageSummary.message.getLogger(), lastMessageSummary.message.getPid(), lastMessageSummary.message.getFile(),
                        lastMessageSummary.message.getLine(), lastMessageSummary.message.getMessage(), lastMessageSummary.count);
            } else {
                LOGGER.log(lastMessageSummary.level, "[{}/{}] [{}@{}] {} | repeated [{}]", lastMessageSummary.message.getLogger(),
                        lastMessageSummary.message.getPid(), lastMessageSummary.message.getFile(), lastMessageSummary.message.getLine(),
                        lastMessageSummary.message.getMessage(), lastMessageSummary.count);
            }
        } else {
            if (jobId != null) {
                LOGGER.log(lastMessageSummary.level, "[{}] [{}/{}] [{}@{}] {}", jobId, lastMessageSummary.message.getLogger(),
                        lastMessageSummary.message.getPid(), lastMessageSummary.message.getFile(), lastMessageSummary.message.getLine(),
                        lastMessageSummary.message.getMessage());
            } else {
                LOGGER.log(lastMessageSummary.level, "[{}/{}] [{}@{}] {}", lastMessageSummary.message.getLogger(),
                        lastMessageSummary.message.getPid(), lastMessageSummary.message.getFile(), lastMessageSummary.message.getLine(),
                        lastMessageSummary.message.getMessage());
            }
        }
    }

    private void storeError(String error) {
        if (Strings.isNullOrEmpty(error) || errorStoreSize <= 0) {
            return;
        }
        if (errorStore.size() >= errorStoreSize) {
            errorStore.removeFirst();
        }
        errorStore.offerLast(error);
    }

    private static int findNextMarker(byte marker, BytesReference bytesRef, int from) {
        for (int i = from; i < bytesRef.length(); ++i) {
            if (bytesRef.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    private static class MessageSummary {
        Instant timestamp;
        int count;
        CppLogMessage message;
        Level level;

        MessageSummary() {
            this.timestamp = Instant.EPOCH;
            this.message = null;
            this.count = 0;
            this.level = Level.OFF;
        }

        void reset(Instant timestamp, CppLogMessage message, Level level) {
            this.timestamp = timestamp;
            this.message = message;
            this.count = 0;
            this.level = level;
        }
    }
}
