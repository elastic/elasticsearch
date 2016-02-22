package org.elasticsearch.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by simon on 2/16/16.
 */
public class IndexClient {

    private final RestClient client;

    public IndexClient(RestClient client) {
        this.client = client;
    }

    public void delete(String index, String type, String id) throws IOException {
        delete(index, type, id, null);
    }
    public void delete(String index, String type, String id, DeleteOptions params) throws IOException {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(id, "id must not be null");
        String deleteEndpoint = String.format("/%s/%s/%s", index, type, id);
        client.httpDelete(deleteEndpoint, params == null ? Collections.emptyMap() : params.options);
    }

    public class DeleteOptions {
        private final Map<String, Object> options = new HashMap<>();
        /** Specific write consistency setting for the operation one of "one", "quorum", "all"*/
        public void consistency(String consistency) {
            options.put("consistency", consistency);
        };
        /** ID of parent document */
        public void parent(String parent){
            options.put("parent", parent);
        };
        /** Refresh the index after performing the operation */
        public void refresh(Boolean refresh) {
            options.put("refresh", refresh);
        };
        /** Specific routing value */
        public void routing(String routing) {
            options.put("routing", routing);
        };
        /** Explicit version number for concurrency control */
        public void version(Number version) {
            options.put("version", version);
        };
        /** Specific version type one of "internal", "external", "external_gte", "force" */
        public void versionType(String versionType) {
            options.put("version_type", versionType);
        };
        /** Explicit operation timeout */
        public void timeout(String timeout) {
            options.put("timeout", timeout);
        };
    }



}
