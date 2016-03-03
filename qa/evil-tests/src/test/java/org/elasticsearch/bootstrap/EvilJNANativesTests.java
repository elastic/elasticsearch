package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EvilJNANativesTests extends ESTestCase {
    public void testSetMaximumNumberOfThreads() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            if (!lines.isEmpty()) {
                for (String line : lines) {
                    if (line != null && line.startsWith("Max processes")) {
                        final String[] fields = line.split("\\s+");
                        final long limit = Long.parseLong(fields[2]);
                        assertThat(JNANatives.MAX_NUMBER_OF_THREADS, equalTo(limit));
                        return;
                    }
                }
            }
            fail("should have read max processes from /proc/self/limits");
        } else {
            assertThat(JNANatives.MAX_NUMBER_OF_THREADS, equalTo(-1L));
        }
    }
}
