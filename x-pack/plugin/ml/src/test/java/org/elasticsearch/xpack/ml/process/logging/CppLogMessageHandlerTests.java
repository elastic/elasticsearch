/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.MockLog.LoggingExpectation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class CppLogMessageHandlerTests extends ESTestCase {

    private static final String TEST_MESSAGE_NOISE = """
        {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 1",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.cc","line":333}
        """;
    private static final String TEST_MESSAGE_NOISE_DIFFERENT_MESSAGE = """
        {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 2",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.cc","line":333}
        """;
    private static final String TEST_MESSAGE_NOISE_DIFFERENT_LEVEL = """
        {"logger":"controller","timestamp":1478261151445,"level":"ERROR","pid":42,"thread":"0x7fff7d2a8000","message":"message 3",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.cc","line":333}
        """;
    private static final String TEST_MESSAGE_OTHER_NOISE = """
        {"logger":"controller","timestamp":1478261151446,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 4",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.h","line":333}
        """;
    private static final String TEST_MESSAGE_SOMETHING = """
        {"logger":"controller","timestamp":1478261151447,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 5",\
        "class":"ml","method":"core::Something","file":"Something.cc","line":555}
        """;
    private static final String TEST_MESSAGE_NOISE_DEBUG = """
        {"logger":"controller","timestamp":1478261151448,"level":"DEBUG","pid":42,"thread":"0x7fff7d2a8000","message":"message 6",\
        "class":"ml","method":"core::SomeNoiseMake","file":"Noisemaker.cc","line":333}
        """;
    private static final String TEST_MESSAGE_NON_JSON_FATAL_ERROR = "Segmentation fault core dumped";

    public void testParse() throws IOException, TimeoutException {
        String testData = """
            {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":10211,"thread":"0x7fff7d2a8000",\
            "message":"uname -a : Darwin Davids-MacBook-Pro.local 15.6.0 Darwin Kernel Version 15.6.0: Thu Sep  1 15:01:16 PDT 2016; \
            root:xnu-3248.60.11~2/RELEASE_X86_64 x86_64","class":"ml","method":"core::CLogger::reconfigureFromProps",\
            "file":"CLogger.cc","line":452}
            {"logger":"controller","timestamp":1478261151445,"level":"DEBUG","pid":10211,"thread":"0x7fff7d2a8000",\
            "message":"Logger is logging to named pipe /var/folders/k5/5sqcdlps5sg3cvlp783gcz740000h0/T/controller_log_784",\
            "class":"ml","method":"core::CLogger::reconfigureLogToNamedPipe","file":"CLogger.cc","line":333}
            {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":10211,"thread":"0x7fff7d2a8000",\
            "message":"controller (64 bit): Version based on 6.0.0-alpha1 (Build b0d6ef8819418c) Copyright (c) 2017 Elasticsearch BV",\
            "method":"main","file":"Main.cc","line":123}
            {"logger":"controller","timestamp":1478261169065,"level":"ERROR","pid":10211,"thread":"0x7fff7d2a8000",\
            "message":"Did not understand verb 'a'","class":"ml","method":"controller::CCommandProcessor::handleCommand",\
            "file":"CCommandProcessor.cc","line":100}
            {"logger":"controller","timestamp":1478261169065,"level":"DEBUG","pid":10211,"thread":"0x7fff7d2a8000",\
            "message":"Ml controller exiting","method":"main","file":"Main.cc","line":147}
            """;

        // Try different buffer sizes to smoke out edge case problems in the buffer management
        for (int readBufSize : new int[] { 11, 42, 101, 1024, 9999 }) {
            InputStream is = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
            try (CppLogMessageHandler handler = new CppLogMessageHandler(is, "_id", readBufSize, 3)) {
                handler.tailStream();

                assertTrue(handler.hasLogStreamEnded());
                // Since this is all being done in one thread and we know the stream has
                // been completely consumed at this point the wait duration can be zero
                assertEquals(10211L, handler.getPid(Duration.ZERO));
                assertEquals(
                    "controller (64 bit): Version based on 6.0.0-alpha1 (Build b0d6ef8819418c) " + "Copyright (c) 2017 Elasticsearch BV",
                    handler.getCppCopyright(Duration.ZERO)
                );
                assertEquals("Did not understand verb 'a'\n", handler.getErrors());
                assertFalse(handler.seenFatalError());
            }
        }
    }

    public void testThrottlingSummary() throws IllegalAccessException, TimeoutException, IOException {

        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DEBUG,
                TEST_MESSAGE_OTHER_NOISE,
                TEST_MESSAGE_SOMETHING
            ).getBytes(StandardCharsets.UTF_8)
        );

        executeLoggingTest(
            is,
            Level.INFO,
            "test_throttling",
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 1"),
            new MockLog.SeenEventExpectation(
                "test2",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 1 | repeated [5]"
            ),
            new MockLog.SeenEventExpectation("test3", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 4"),
            new MockLog.SeenEventExpectation("test4", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 5")
        );
    }

    public void testThrottlingSummaryOneRepeat() throws IllegalAccessException, TimeoutException, IOException {

        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DEBUG,
                TEST_MESSAGE_OTHER_NOISE,
                TEST_MESSAGE_SOMETHING
            ).getBytes(StandardCharsets.UTF_8)
        );

        executeLoggingTest(
            is,
            Level.INFO,
            "test_throttling",
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 1"),
            new MockLog.UnseenEventExpectation(
                "test2",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 1 | repeated [1]"
            ),
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 4"),
            new MockLog.SeenEventExpectation("test2", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 5")
        );
    }

    public void testThrottlingSummaryLevelChanges() throws IllegalAccessException, TimeoutException, IOException {

        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DIFFERENT_LEVEL,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DEBUG,
                TEST_MESSAGE_OTHER_NOISE,
                TEST_MESSAGE_SOMETHING
            ).getBytes(StandardCharsets.UTF_8)
        );

        executeLoggingTest(
            is,
            Level.INFO,
            "test_throttling",
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 1"),
            new MockLog.SeenEventExpectation(
                "test2",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 1 | repeated [2]"
            ),
            new MockLog.SeenEventExpectation("test3", CppLogMessageHandler.class.getName(), Level.ERROR, "[test_throttling] * message 3"),
            new MockLog.SeenEventExpectation(
                "test4",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 1 | repeated [3]"
            ),
            new MockLog.SeenEventExpectation("test5", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 4"),
            new MockLog.SeenEventExpectation("test6", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 5")
        );
    }

    public void testThrottlingLastMessageRepeast() throws IllegalAccessException, TimeoutException, IOException {

        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DIFFERENT_MESSAGE
            ).getBytes(StandardCharsets.UTF_8)
        );

        executeLoggingTest(
            is,
            Level.INFO,
            "test_throttling",
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 1"),
            new MockLog.SeenEventExpectation(
                "test2",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 2 | repeated [5]"
            )
        );
    }

    public void testThrottlingDebug() throws IllegalAccessException, TimeoutException, IOException {

        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DEBUG
            ).getBytes(StandardCharsets.UTF_8)
        );

        executeLoggingTest(
            is,
            Level.DEBUG,
            "test_throttling",
            new MockLog.SeenEventExpectation("test1", CppLogMessageHandler.class.getName(), Level.INFO, "[test_throttling] * message 1"),
            new MockLog.SeenEventExpectation("test2", CppLogMessageHandler.class.getName(), Level.DEBUG, "[test_throttling] * message 6"),
            new MockLog.UnseenEventExpectation(
                "test3",
                CppLogMessageHandler.class.getName(),
                Level.INFO,
                "[test_throttling] * message 1 | repeated [5]"
            )
        );
    }

    public void testWaitForLogStreamClose() throws IOException {
        InputStream is = new ByteArrayInputStream(
            String.join(
                "",
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE,
                TEST_MESSAGE_NOISE_DIFFERENT_MESSAGE
            ).getBytes(StandardCharsets.UTF_8)
        );

        try (CppLogMessageHandler handler = new CppLogMessageHandler("test_throttling", is)) {
            handler.tailStream();
            assertTrue(handler.waitForLogStreamClose(Duration.ofMillis(100)));
            assertTrue(handler.hasLogStreamEnded());
        }
    }

    public void testParseFatalError() throws IOException, IllegalAccessException {
        InputStream is = new ByteArrayInputStream(TEST_MESSAGE_NON_JSON_FATAL_ERROR.getBytes(StandardCharsets.UTF_8));

        try (CppLogMessageHandler handler = new CppLogMessageHandler("test_error", is)) {
            is.close();
            handler.tailStream();
            assertTrue(handler.seenFatalError());
            assertTrue(handler.getErrors().contains(TEST_MESSAGE_NON_JSON_FATAL_ERROR));
            assertTrue(handler.getErrors().contains("Fatal error"));
        }
    }

    private static void executeLoggingTest(InputStream is, Level level, String jobId, LoggingExpectation... expectations)
        throws IOException {
        Logger cppMessageLogger = LogManager.getLogger(CppLogMessageHandler.class);
        Level oldLevel = cppMessageLogger.getLevel();

        MockLog.assertThatLogger(() -> {
            Loggers.setLevel(cppMessageLogger, level);
            try (CppLogMessageHandler handler = new CppLogMessageHandler(jobId, is)) {
                handler.tailStream();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                Loggers.setLevel(cppMessageLogger, oldLevel);
            }
        }, CppLogMessageHandler.class, expectations);
    }
}
