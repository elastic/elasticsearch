package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 */
public class LookupScript extends AbstractSearchScript {

    /**
     * Native scripts are build using factories that are registered in the
     * {@link org.elasticsearch.examples.nativescript.plugin.NativeScriptExamplesPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when plugin is loaded.
     */
    public static class Factory extends AbstractComponent implements NativeScriptFactory{

        private final Node node;

        private final Cache<Tuple<String, String>, Map<String, Object>> cache;

        /**
         * This constructor will be called by guice during initialization
         *
         * @param node injecting the reference to current node to get access to node's client
         */
        @SuppressWarnings("unchecked")
        @Inject
        public Factory(Node node, Settings settings) {
            super(settings);
            // Node is not fully initialized here
            // All we can do is save a reference to it for future use
            this.node = node;

            // Setup lookup cache
            ByteSizeValue size = settings.getAsBytesSize("examples.nativescript.lookup.size", null);
            TimeValue expire = settings.getAsTime("expire", null);
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
            if (size != null) {
                cacheBuilder.maximumSize(size.bytes());
            }
            if (expire != null) {
                cacheBuilder.expireAfterAccess(expire.nanos(), TimeUnit.NANOSECONDS);
            }
            cache = cacheBuilder.build();
        }

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            if (params == null) {
                throw new ScriptException("Missing script parameters");
            }
            String lookupIndex = XContentMapValues.nodeStringValue(params.get("lookup_index"), null);
            if (lookupIndex == null) {
                throw new ScriptException("Missing the index parameter");
            }
            String lookupType = XContentMapValues.nodeStringValue(params.get("lookup_type"), null);
            if (lookupType == null) {
                throw new ScriptException("Missing the index parameter");
            }
            String field = XContentMapValues.nodeStringValue(params.get("field"), null);
            if (field == null) {
                throw new ScriptException("Missing the field parameter");
            }
            return new LookupScript(node.client(), logger, cache, lookupIndex, lookupType, field);
        }
    }

    private final String lookupIndex;
    private final String lookupType;
    private final String field;
    private final ESLogger logger;

    private final Client client;
    private final Cache<Tuple<String, String>, Map<String, Object>> cache;

    private static final Map<String, Object> EMPTY_MAP = ImmutableMap.of();

    private LookupScript(Client client, ESLogger logger, Cache<Tuple<String, String>, Map<String, Object>> cache, String lookupIndex, String lookupType, String field) {
        this.client = client;
        this.logger = logger;
        this.lookupIndex = lookupIndex;
        this.lookupType = lookupType;
        this.field = field;
        this.cache = cache;
    }

    @Override
    public Object run() {
        // First we get field using doc lookup
        ScriptDocValues  docValue = (ScriptDocValues)doc().get(field);
        // This is not very efficient
        // Check if field exists
        if (docValue != null && !docValue.isEmpty()) {
            final String fieldValue = ((ScriptDocValues.Strings) docValue).getValue();
            if (fieldValue != null) {
                try {
                    return cache.get(new Tuple<String, String>(lookupIndex + "/" + lookupType, fieldValue), new Callable<Map<String, Object>>() {
                        @Override
                        public Map<String, Object> call() throws Exception {
                            // This is not very efficient of doing this, but it demonstrates using injected client
                            // for record lookup
                            GetResponse response = client.prepareGet(lookupIndex, lookupType, fieldValue).setPreference("_local").execute().actionGet();
                            if (logger.isTraceEnabled()) {
                                logger.trace("lookup [{}]/[{}]/[{}], found: [{}]", lookupIndex, lookupType, fieldValue, response.isExists());
                            }
                            if (response.isExists()) {
                                return response.getSource();
                            }
                            return EMPTY_MAP;
                        }
                    });

                } catch (ExecutionException ex) {
                    throw new ScriptException("Lookup failure ", ex);
                }
            }
        }
        return null;
    }
}
