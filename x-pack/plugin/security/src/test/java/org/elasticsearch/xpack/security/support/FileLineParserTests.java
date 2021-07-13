/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FileLineParserTests extends ESTestCase {

    public void testParse() throws IOException {
        Path path = getDataPath("../authc/support/role_mapping.yml");

        final Map<Integer, String> lines = new HashMap<>(Map.of(
            7, "security:",
            8, "  - \"cn=avengers,ou=marvel,o=superheros\"",
            9, "  - \"cn=shield,ou=marvel,o=superheros\"",
            10, "avenger:",
            11, "  - \"cn=avengers,ou=marvel,o=superheros\"",
            12, "  - \"cn=Horatio Hornblower,ou=people,o=sevenSeas\""
        ));

        FileLineParser.parse(path, (lineNumber, line) -> {
            assertThat(lines.remove(lineNumber), equalTo(line));
        });
        assertThat(lines.isEmpty(), is(true));
    }
}
