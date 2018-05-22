package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.MultiValueMode;

import java.util.HashMap;
import java.util.Map;

public class MultiValuesSourceConfig<VS extends ValuesSource> {
    private Map<String, Wrapper<VS>> map = new HashMap<>();

    public static class Wrapper<VS extends ValuesSource> {
        private MultiValueMode multi;
        private ValuesSourceConfig<VS> config;

        public Wrapper(MultiValueMode multi, ValuesSourceConfig<VS> config) {
            this.multi = multi;
            this.config = config;
        }

        public MultiValueMode getMulti() {
            return multi;
        }

        public ValuesSourceConfig<VS> getConfig() {
            return config;
        }
    }

    public void addField(String fieldName, ValuesSourceConfig<VS> config, MultiValueMode multiValueMode) {
        map.put(fieldName, new Wrapper<>(multiValueMode, config));
    }

    public Map<String, Wrapper<VS>> getMap() {
        return map;
    }

}
