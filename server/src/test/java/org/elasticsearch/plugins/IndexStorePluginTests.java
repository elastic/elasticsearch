package org.elasticsearch.plugins;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.hasToString;

public class IndexStorePluginTests extends ESTestCase {

    public static class BarStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreProviders() {
            return Collections.singletonMap("store", IndexStore::new);
        }

    }

    public static class FooStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreProviders() {
            return Collections.singletonMap("store", IndexStore::new);
        }

    }

    public void testDuplicateIndexStoreProviders() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
                IllegalStateException.class, () -> new MockNode(settings, Arrays.asList(BarStorePlugin.class, FooStorePlugin.class)));
        assertThat(e, hasToString(matches(
                "java.lang.IllegalStateException: Duplicate key store \\(attempted merging values " +
                        "org.elasticsearch.plugins.IndexStorePluginTests\\$BarStorePlugin.* " +
                        "and org.elasticsearch.plugins.IndexStorePluginTests\\$FooStorePlugin.*\\)")));
    }

}
