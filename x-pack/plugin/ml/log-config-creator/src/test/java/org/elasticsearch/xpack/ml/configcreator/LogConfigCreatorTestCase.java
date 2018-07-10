/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public abstract class LogConfigCreatorTestCase extends ESTestCase {

    protected static final List<String> POSSIBLE_CHARSETS = Collections.unmodifiableList(Charset.availableCharsets().keySet().stream()
        .filter(name -> LogFileStructureFinder.FILEBEAT_SUPPORTED_ENCODINGS.contains(name.toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList()));
    protected static final List<String> POSSIBLE_TIMEZONES = Arrays.asList(null, "Europe/London", "UTC", "+05:30", "-08:00", "EST");
    protected static final List<String> POSSIBLE_HOSTNAMES = Arrays.asList("localhost", "192.168.1.2", "::1", "server", "server.acme.com");

    protected static final Terminal TEST_TERMINAL;
    static {
        TEST_TERMINAL = Terminal.DEFAULT;
        TEST_TERMINAL.setVerbosity(Verbosity.VERBOSE);
    }

    protected static final String TEST_FILE_NAME = "a_test_file";
    protected static final String TEST_INDEX_NAME = "test";

    protected static final String CSV_SAMPLE = "time,id,value\n" +
        "2018-05-17T16:23:40,key1,42.0\n" +
        "2018-05-17T16:24:11,\"key with spaces\",42.0\n";

    protected static final String JSON_SAMPLE = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\"," +
            "\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 1\",\"class\":\"ml\"," +
            "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n" +
        "{\"logger\":\"controller\",\"timestamp\":1478261151445," +
            "\"level\":\"INFO\",\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 2\",\"class\":\"ml\"," +
            "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n";

    protected static final String TEXT_SAMPLE = "[2018-05-11T17:07:29,461][INFO ][o.e.n.Node               ] [node-0] initializing ...\n" +
        "[2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], " +
            "net usable_space [223.4gb], net total_space [464.7gb], types [hfs]\n" +
        "[2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], " +
            "compressed ordinary object pointers [true]\n" +
        "[2018-05-11T17:07:29,556][INFO ][o.e.n.Node               ] [node-0] node name [node-0], node ID [tJ9u8HcaTbWxRtnlfz1RQA]\n";

    protected static final String TSV_SAMPLE = "time\tid\tvalue\n" +
        "2018-05-17T16:23:40\tkey1\t42.0\n" +
        "2018-05-17T16:24:11\t\"key with spaces\"\t42.0\n";

    protected static final String XML_SAMPLE = "<log4j:event logger=\"autodetect\" timestamp=\"1526574809521\" level=\"ERROR\" " +
            "thread=\"0x7fffc5a7c3c0\">\n" +
        "<log4j:message><![CDATA[Neither a fieldname clause nor a field config file was specified]]></log4j:message>\n" +
        "</log4j:event>\n" +
        "\n" +
        "<log4j:event logger=\"autodetect\" timestamp=\"1526574809522\" level=\"FATAL\" thread=\"0x7fffc5a7c3c0\">\n" +
        "<log4j:message><![CDATA[Field config could not be interpreted]]></log4j:message>\n" +
        "</log4j:event>\n" +
        "\n";
}
