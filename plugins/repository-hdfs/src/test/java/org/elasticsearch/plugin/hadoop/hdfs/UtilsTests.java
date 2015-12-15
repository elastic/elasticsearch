package org.elasticsearch.plugin.hadoop.hdfs;

import org.elasticsearch.test.ESTestCase;

public class UtilsTests extends ESTestCase {

    public void testDetectLibFolder() {
        String location = HdfsPlugin.class.getProtectionDomain().getCodeSource().getLocation().toString();
        assertEquals(location, Utils.detectLibFolder());
    }
}
