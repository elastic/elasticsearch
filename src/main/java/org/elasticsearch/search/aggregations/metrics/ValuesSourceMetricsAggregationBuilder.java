package org.elasticsearch.search.aggregations.metrics;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class ValuesSourceMetricsAggregationBuilder<B extends ValuesSourceMetricsAggregationBuilder<B>> extends MetricsAggregationBuilder<B> {

    private String field;
    private String script;
    private String scriptLang;
    private Map<String, Object> params;

    protected ValuesSourceMetricsAggregationBuilder(String name, String type) {
        super(name, type);
    }

    @SuppressWarnings("unchecked")
    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B script(String script) {
        this.script = script;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B param(String name, Object value) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.put(name, value);
        return (B) this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }

        if (script != null) {
            builder.field("script", script);
        }

        if (scriptLang != null) {
            builder.field("script_lang", scriptLang);
        }

        if (this.params != null && !this.params.isEmpty()) {
            builder.field("params").map(this.params);
        }
    }
}
