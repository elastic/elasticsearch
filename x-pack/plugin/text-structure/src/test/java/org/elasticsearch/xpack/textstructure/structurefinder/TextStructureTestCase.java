/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public abstract class TextStructureTestCase extends ESTestCase {

    protected static final List<String> POSSIBLE_CHARSETS = Collections.unmodifiableList(
        Charset.availableCharsets()
            .keySet()
            .stream()
            .filter(name -> TextStructureFinderManager.FILEBEAT_SUPPORTED_ENCODINGS.contains(name.toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList())
    );

    protected static final String CSV_SAMPLE = """
        time,id,value
        2018-05-17T16:23:40,key1,42.0
        2018-05-17T16:24:11,"key with spaces",42.0
        """;
    protected static final String NDJSON_SAMPLE = """
        {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 1",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.cc","line":333}
        {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 2",\
        "class":"ml","method":"core::SomeNoiseMaker","file":"Noisemaker.cc","line":333}
        """;

    protected static final String PIPE_DELIMITED_SAMPLE = """
        2018-01-06 16:56:14.295748|INFO    |VirtualServer |1  |listening on 0.0.0.0:9987, :::9987
        2018-01-06 17:19:44.465252|INFO    |VirtualServer |1  |client 'User1'(id:2) changed default admin channelgroup to 'Guest'(id:8)
        2018-01-06 17:21:25.764368|INFO    |VirtualServer |1  |client 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by \
        client 'User1'(id:2) in channel 'Default Channel'(id:1)""";

    protected static final String SEMI_COLON_DELIMITED_SAMPLE = """
        "pos_id";"trip_id";"latitude";"longitude";"altitude";"timestamp"
        "1";"3";"4703.7815";"1527.4713";"359.9";"2017-01-19 16:19:04.742113"
        "2";"3";"4703.7815";"1527.4714";"359.9";"2017-01-19 16:19:05.741890"
        "3";"3";"4703.7816";"1527.4716";"360.3";"2017-01-19 16:19:06.738842
        """;

    protected static final String TEXT_SAMPLE = """
        [2018-05-11T17:07:29,461][INFO ][o.e.n.Node               ] [node-0] initializing ...
        [2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], net \
        usable_space [223.4gb], net total_space [464.7gb], types [hfs]
        [2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], compressed ordinary object pointers \
        [true]
        [2018-05-11T17:07:29,556][INFO ][o.e.n.Node               ] [node-0] node name [node-0], node ID [tJ9u8HcaTbWxRtnlfz1RQA]
        """;

    protected static final String TEXT_WITH_NO_TIMESTAMPS_SAMPLE = """
        [INFO ][o.e.n.Node               ] [node-0] initializing ...
        [INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], net \
        usable_space [223.4gb], net total_space [464.7gb], types [hfs]
        [INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], compressed ordinary object pointers [true]
        [INFO ][o.e.n.Node               ] [node-0] node name [node-0], node ID [tJ9u8HcaTbWxRtnlfz1RQA]
        """;

    protected static final String TSV_SAMPLE = """
        time\tid\tvalue
        2018-05-17T16:23:40\tkey1\t42.0
        2018-05-17T16:24:11\t"key with spaces"\t42.0
        """;

    protected static final String XML_SAMPLE = """
        <log4j:event logger="autodetect" timestamp="1526574809521" level="ERROR" thread="0x7fffc5a7c3c0">
        <log4j:message><![CDATA[Neither a fieldname clause nor a field config file was specified]]></log4j:message>
        </log4j:event>

        <log4j:event logger="autodetect" timestamp="1526574809522" level="FATAL" thread="0x7fffc5a7c3c0">
        <log4j:message><![CDATA[Field config could not be interpreted]]></log4j:message>
        </log4j:event>

        """;

    // This doesn't need closing because it has an infinite timeout
    protected static final TimeoutChecker NOOP_TIMEOUT_CHECKER = new TimeoutChecker("unit test", null, null);

    protected List<String> explanation;

    @Before
    public void initExplanation() {
        explanation = new ArrayList<>();
    }

    @After
    public void printExplanation() {
        LogManager.getLogger(getClass()).info("Explanation:\n" + String.join("\n", explanation));
    }

    protected Boolean randomHasByteOrderMarker(String charset) {
        return charset.toUpperCase(Locale.ROOT).startsWith("UTF") ? randomBoolean() : null;
    }
}
