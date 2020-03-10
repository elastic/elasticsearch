package org.elasticsearch.rest;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ToXContent;

import java.util.function.Consumer;

public class CompatibleHandlers {

    /**
     * TODO revisit when https://github.com/elastic/elasticsearch/issues/52370 is resolved
     */
    public static final String COMPATIBLE_HEADER = "Accept";
    public static final String COMPATIBLE_PARAMS_KEY = "Compatible-With";
    public static final String COMPATIBLE_VERSION = "7";

    public static Consumer<RestRequest> consumeParameterType(DeprecationLogger deprecationLogger) {
        String TYPES_DEPRECATION_MESSAGE = "[types removal] Using type as a path parameter is deprecated.";

        return r -> {
            deprecationLogger.deprecatedAndMaybeLog("create_index_with_types", TYPES_DEPRECATION_MESSAGE);
            r.param("type");
        };
    }

    public static boolean isV7Compatible(ToXContent.Params params) {
        String param = params.param(COMPATIBLE_PARAMS_KEY);
        return COMPATIBLE_VERSION.equals(param);
    }

}
