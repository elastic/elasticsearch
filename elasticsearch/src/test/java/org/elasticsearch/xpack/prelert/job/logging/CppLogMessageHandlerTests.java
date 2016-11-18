/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class CppLogMessageHandlerTests extends ESTestCase {

    public void testParse() throws IOException {

        String testData = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\",\"pid\":10211,"
                + "\"thread\":\"0x7fff7d2a8000\",\"message\":\"uname -a : Darwin Davids-MacBook-Pro.local 15.6.0 Darwin Kernel "
                + "Version 15.6.0: Thu Sep  1 15:01:16 PDT 2016; root:xnu-3248.60.11~2/RELEASE_X86_64 x86_64\",\"class\":\"prelert\","
                + "\"method\":\"core::CLogger::reconfigureFromProps\",\"file\":\"CLogger.cc\",\"line\":452}\n"
                + "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"DEBUG\",\"pid\":10211,\"thread\":\"0x7fff7d2a8000\","
                + "\"message\":\"Logger is logging to named pipe "
                + "/var/folders/k5/5sqcdlps5sg3cvlp783gcz740000h0/T/prelert_controller_log_784\",\"class\":\"prelert\","
                + "\"method\":\"core::CLogger::reconfigureLogToNamedPipe\",\"file\":\"CLogger.cc\",\"line\":333}\n"
                + "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\",\"pid\":10211,\"thread\":\"0x7fff7d2a8000\","
                + "\"message\":\"prelert_controller (64 bit): Version based on 6.5.0 (Build DEVELOPMENT BUILD by dave) "
                + "Copyright (c) Prelert Ltd 2006-2016\",\"method\":\"main\",\"file\":\"Main.cc\",\"line\":123}\n"
                + "{\"logger\":\"controller\",\"timestamp\":1478261169065,\"level\":\"ERROR\",\"pid\":10211,\"thread\":\"0x7fff7d2a8000\","
                + "\"message\":\"Did not understand verb 'a'\",\"class\":\"prelert\","
                + "\"method\":\"controller::CCommandProcessor::handleCommand\",\"file\":\"CCommandProcessor.cc\",\"line\":100}\n"
                + "{\"logger\":\"controller\",\"timestamp\":1478261169065,\"level\":\"DEBUG\",\"pid\":10211,\"thread\":\"0x7fff7d2a8000\","
                + "\"message\":\"Prelert controller exiting\",\"method\":\"main\",\"file\":\"Main.cc\",\"line\":147}\n";

        InputStream is = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));

        Logger logger = Loggers.getLogger(CppLogMessageHandlerTests.class);
        Loggers.setLevel(logger, Level.DEBUG);

        try (CppLogMessageHandler handler = new CppLogMessageHandler(is, logger, 100, 3)) {
            handler.tailStream();

            assertEquals("Did not understand verb 'a'\n", handler.getErrors());
        }
    }
}
