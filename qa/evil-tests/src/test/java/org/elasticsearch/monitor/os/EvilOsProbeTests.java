/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.os;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class EvilOsProbeTests extends ESTestCase {

    public void testOsPrettyName() throws IOException {
        final OsInfo osInfo = OsProbe.getInstance().osInfo(randomLongBetween(1, 100), Processors.of((double) randomIntBetween(1, 8)));
        if (Constants.LINUX) {
            final List<String> lines;
            if (Files.exists(PathUtils.get("/etc/os-release"))) {
                lines = Files.readAllLines(PathUtils.get("/etc/os-release"));
            } else if (Files.exists(PathUtils.get("/usr/lib/os-release"))) {
                lines = Files.readAllLines(PathUtils.get("/usr/lib/os-release"));
            } else {
                lines = Collections.singletonList(
                    "PRETTY_NAME=\"" + Files.readAllLines(PathUtils.get("/etc/system-release")).get(0) + "\""
                );
            }
            for (final String line : lines) {
                if (line != null && line.startsWith("PRETTY_NAME=")) {
                    final Matcher matcher = Pattern.compile("PRETTY_NAME=(\"?|'?)?([^\"']+)\\1").matcher(line.trim());
                    final boolean matches = matcher.matches();
                    assert matches : line;
                    assert matcher.groupCount() == 2 : line;
                    final String prettyName = matcher.group(2);
                    assertThat(osInfo.getPrettyName(), equalTo(prettyName));
                    return;
                }
            }
            assertThat(osInfo.getPrettyName(), equalTo("Linux"));
        } else {
            assertThat(osInfo.getPrettyName(), equalTo(Constants.OS_NAME));
        }
    }

}
