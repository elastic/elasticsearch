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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaxMapCountCheckTests extends ESTestCase {

    public void testGetMaxMapCountOnLinux() {
        if (Constants.LINUX) {
            final BootstrapCheck.MaxMapCountCheck check = new BootstrapCheck.MaxMapCountCheck();
            assertThat(check.getMaxMapCount(), greaterThan(0L));
        }
    }

    public void testGetMaxMapCount() throws IOException {
        final long procSysVmMaxMapCount = randomIntBetween(1, Integer.MAX_VALUE);
        final BufferedReader reader = mock(BufferedReader.class);
        when(reader.readLine()).thenReturn(Long.toString(procSysVmMaxMapCount));
        final Path procSysVmMaxMapCountPath = PathUtils.get("/proc/sys/vm/max_map_count");
        BootstrapCheck.MaxMapCountCheck check = new BootstrapCheck.MaxMapCountCheck() {
            @Override
            BufferedReader getBufferedReader(Path path) throws IOException {
                assertEquals(path, procSysVmMaxMapCountPath);
                return reader;
            }
        };

        assertThat(check.getMaxMapCount(), equalTo(procSysVmMaxMapCount));
        verify(reader).close();

        reset(reader);
        final IOException ioException = new IOException("fatal");
        when(reader.readLine()).thenThrow(ioException);
        final ESLogger logger = mock(ESLogger.class);
        assertThat(check.getMaxMapCount(logger), equalTo(-1L));
        verify(logger).warn("I/O exception while trying to read [{}]", ioException, procSysVmMaxMapCountPath);
        verify(reader).close();

        reset(reader);
        reset(logger);
        when(reader.readLine()).thenReturn("eof");
        assertThat(check.getMaxMapCount(logger), equalTo(-1L));
        verify(logger).warn(eq("unable to parse vm.max_map_count [{}]"), any(NumberFormatException.class), eq("eof"));
        verify(reader).close();
    }

    public void testMaxMapCountCheckRead() throws IOException {
        final String rawProcSysVmMaxMapCount = Long.toString(randomIntBetween(1, Integer.MAX_VALUE));
        final BufferedReader reader = mock(BufferedReader.class);
        when(reader.readLine()).thenReturn(rawProcSysVmMaxMapCount);
        final BootstrapCheck.MaxMapCountCheck check = new BootstrapCheck.MaxMapCountCheck();
        assertThat(check.readProcSysVmMaxMapCount(reader), equalTo(rawProcSysVmMaxMapCount));
    }

    public void testMaxMapCountCheckParse() {
        final long procSysVmMaxMapCount = randomIntBetween(1, Integer.MAX_VALUE);
        final BootstrapCheck.MaxMapCountCheck check = new BootstrapCheck.MaxMapCountCheck();
        assertThat(check.parseProcSysVmMaxMapCount(Long.toString(procSysVmMaxMapCount)), equalTo(procSysVmMaxMapCount));
    }

}
