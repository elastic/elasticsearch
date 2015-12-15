package org.elasticsearch.plugin.hadoop.hdfs;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.plugin.hadoop.hdfs.HdfsPlugin;

public class HdfsTestPlugin extends HdfsPlugin {

    @Override
    protected List<URL> getHadoopClassLoaderPath(String baseLib) {
        return Collections.emptyList();
    }
}
