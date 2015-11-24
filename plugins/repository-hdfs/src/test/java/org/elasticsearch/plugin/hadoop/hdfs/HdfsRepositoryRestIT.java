package org.elasticsearch.plugin.hadoop.hdfs;

import java.io.IOException;
import java.util.Collection;

import org.elasticsearch.plugin.hadoop.hdfs.HdfsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class HdfsRepositoryRestIT extends ESRestTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(HdfsPlugin.class);
    }

    public HdfsRepositoryRestIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }
}
