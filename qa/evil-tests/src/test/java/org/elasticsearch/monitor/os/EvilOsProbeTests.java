/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.os;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class EvilOsProbeTests extends ESTestCase {

    public void testOsPrettyName() throws IOException  {
        final OsInfo osInfo = OsProbe.getInstance().osInfo(randomLongBetween(1, 100), randomIntBetween(1, 8));
        if (Constants.LINUX) {
            final List<String> lines;
            if (Files.exists(PathUtils.get("/etc/os-release"))) {
                lines = Files.readAllLines(PathUtils.get("/etc/os-release"));
            } else if (Files.exists(PathUtils.get("/usr/lib/os-release"))) {
                lines = Files.readAllLines(PathUtils.get("/usr/lib/os-release"));
            } else {
                lines = Collections.singletonList(
                        "PRETTY_NAME=\"" + Files.readAllLines(PathUtils.get("/etc/system-release")).get(0) + "\"");
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
