package org.elasticsearch.plugins;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStore;

import java.util.Map;
import java.util.function.Function;

public interface IndexStorePlugin {

    Map<String, Function<IndexSettings, IndexStore>> getIndexStoreProviders();

}
